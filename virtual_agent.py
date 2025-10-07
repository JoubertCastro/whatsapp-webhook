from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
import requests
from requests.adapters import HTTPAdapter, Retry
import yaml

from datetime import datetime, time as dtime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except ImportError:
    ZoneInfo = None

# ==========================
# Paths / Base
# ==========================
BASE_DIR = Path(__file__).resolve().parent

# ==========================
# Config via ambiente
# ==========================
DATABASE_URL   = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
PHONE_ID       = os.getenv("PHONE_ID")
GRAPH_VERSION  = os.getenv("GRAPH_VERSION", "v23.0")
WABA_ID        = os.getenv("WABA_ID", "")

# Restrição opcional por conta/phone_id (se definido, o webhook só processa esses IDs)
ALLOWED_PHONE_IDS = set(filter(None, os.getenv("ALLOWED_PHONE_IDS", "").split(",")))  # ex: "732661079928516"
ALLOWED_WABA_IDS  = set(filter(None, os.getenv("ALLOWED_WABA_IDS", "").split(",")))   # ex: "1910445533050310"

# HTTP tuning
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "15"))
RETRY_MAX      = int(os.getenv("RETRY_MAX", "3"))

# Sanitização de ctx após webhooks externos
SAFE_KEYS = set(filter(None, (os.getenv("FLOW_SAFE_CTX_KEYS", "id,status,valor,nome").split(","))))
SAFE_STR_MAXLEN = int(os.getenv("FLOW_SAFE_STR_MAXLEN", "500"))

# ==========================
# Conexão Postgres (pool)
# ==========================
POOL = SimpleConnectionPool(
    1, int(os.getenv("PG_MAXCONN", "10")),
    dsn=DATABASE_URL,
    cursor_factory=psycopg2.extras.RealDictCursor
)

class DB:
    def __enter__(self):
        self.conn = POOL.getconn()
        return self.conn
    def __exit__(self, exc_type, exc, tb):
        POOL.putconn(self.conn)

def get_conn():
    """Compatível com 'with get_conn() as conn'."""
    return DB()

# ==========================
# HTTP Session com Retry
# ==========================
_http = requests.Session()
_http.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=RETRY_MAX,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=["GET", "POST"]
        )
    )
)

def _post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], int]:
    r = _http.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT_S)
    try:
        content_type = r.headers.get("Content-Type", "")
        body = r.json() if "application/json" in content_type else {"raw": r.text}
        return r.ok, body, r.status_code
    finally:
        r.close()

def _get_json(url: str, headers: Dict[str, str], params: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], int]:
    r = _http.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT_S)
    try:
        content_type = r.headers.get("Content-Type", "")
        body = r.json() if "application/json" in content_type else {"raw": r.text}
        return r.ok, body, r.status_code
    finally:
        r.close()

# ==========================
# Auxiliares WhatsApp API
# ==========================
def send_wa_text(to: str, body: str, phone_id: Optional[str] = None, token: Optional[str] = None) -> Dict[str, Any]:
    pid = phone_id or PHONE_ID
    tok = token or WHATSAPP_TOKEN
    if not pid:
        return {"error": {"message": "PHONE_ID ausente"}}
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{pid}/messages"
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    ok, resp, _ = _post_json(url, headers, {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"preview_url": False, "body": body[:4000]},
    })
    return resp

def send_wa_buttons(to: str, body: str, options: List[Tuple[str, str]], phone_id: Optional[str] = None, token: Optional[str] = None) -> Dict[str, Any]:
    pid = phone_id or PHONE_ID
    tok = token or WHATSAPP_TOKEN
    if not pid:
        return {"error": {"message": "PHONE_ID ausente"}}
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{pid}/messages"
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    buttons = [
        {"type": "reply", "reply": {"id": str(v)[:256], "title": str(l)[:20]}}
        for (l, v) in options[:3]
    ]
    ok, resp, _ = _post_json(url, headers, {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body[:1024]},
            "action": {"buttons": buttons},
        },
    })
    return resp

# ==========================
# Motor de fluxo
# ==========================
@dataclass
class FlowNode:
    id: str
    type: str
    data: Dict[str, Any]

class Flow:
    def __init__(self, spec: Dict[str, Any]):
        self.flow_id = spec["flow_id"]
        self.start = spec["start"]
        self.intents = spec.get("intents", {})
        self.nodes: Dict[str, FlowNode] = {
            nid: FlowNode(id=nid, type=nd.get("type"), data=nd)
            for nid, nd in spec["nodes"].items()
        }

# Cache por mtime (evita IO/parsing por mensagem)
from functools import lru_cache
import os as _os

@lru_cache(maxsize=64)
def _load_flow_cached(path_str: str, mtime: float) -> Flow:
    with open(path_str, "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)
    return Flow(spec)

def _safe_flow_path(path: str) -> Path:
    p = Path(path)
    if not p.is_absolute():
        p = BASE_DIR / p
    # impede path traversal
    p = p.resolve()
    if not str(p).startswith(str(BASE_DIR)):
        raise ValueError("flow_file fora do diretório permitido")
    return p

def load_flow_from_file(path: str) -> Flow:
    p = _safe_flow_path(path)
    st = _os.stat(p)
    return _load_flow_cached(str(p), st.st_mtime)

# ==========================
# Sessão e armazenamento
# ==========================
@dataclass
class Session:
    wa_phone: str
    flow_id: str
    node_id: str
    ctx: Dict[str, Any] = field(default_factory=dict)
    contact: Dict[str, Any] = field(default_factory=dict)
    assigned: str = "virtual"

class Store:
    @staticmethod
    def get_session(wa_phone: str) -> Optional[Session]:
        # Lock pessimista evita interleaving entre mensagens simultâneas
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM bot_sessions
                 WHERE wa_phone=%s
                 FOR UPDATE
            """, (wa_phone,))
            row = cur.fetchone()
            if not row:
                return None
            return Session(
                wa_phone=row["wa_phone"],
                flow_id=row["flow_id"],
                node_id=row["node_id"],
                ctx=row.get("ctx") or {},
                contact=row.get("contact") or {},
                assigned=row.get("assigned", "virtual"),
            )

    @staticmethod
    def upsert_session(s: Session) -> None:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bot_sessions (wa_phone, flow_id, node_id, ctx, contact, assigned, updated_at)
                VALUES (%s,%s,%s,%s::jsonb,%s::jsonb,%s,NOW())
                ON CONFLICT (wa_phone) DO UPDATE SET
                  flow_id   = EXCLUDED.flow_id,
                  node_id   = EXCLUDED.node_id,
                  ctx       = EXCLUDED.ctx,
                  contact   = EXCLUDED.contact,
                  assigned  = EXCLUDED.assigned,
                  updated_at= NOW()
            """, (s.wa_phone, s.flow_id, s.node_id, json.dumps(s.ctx), json.dumps(s.contact), s.assigned))

    @staticmethod
    def log(wa_phone: str, direction: str, payload: Dict[str, Any]) -> None:
        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO bot_logs (wa_phone, direction, payload) VALUES (%s,%s,%s::jsonb)",
                    (wa_phone, direction, json.dumps(payload)),
                )
        except Exception:
            pass

# ==========================
# Helpers de template
# ==========================
def render_text(tpl: str, ctx: Dict[str, Any]) -> str:
    def repl(m: re.Match) -> str:
        expr = m.group(1).strip()
        fallback = ""
        if " or " in expr:
            left, right = expr.split(" or ", 1)
            expr = left.strip()
            mfb = re.match(r'^[\'"](.*?)[\'"]$', right.strip())
            if mfb:
                fallback = mfb.group(1)
        parts = expr.split(".")
        val: Any = ctx
        for p in parts:
            val = val.get(p) if isinstance(val, dict) else None
            if val is None:
                break
        return str(val) if val is not None else fallback
    return re.sub(r"\{\{([^}]+)\}\}", repl, tpl)

# ==========================
# Regras de horário comercial
# ==========================
def _now_in_tz(tz_name: str) -> datetime:
    if ZoneInfo:
        return datetime.now(ZoneInfo(tz_name))
    # fallback: assume UTC-3
    return datetime.now(timezone.utc) - timedelta(hours=3)

def _is_business_hours(dt: datetime) -> bool:
    # Segunda=0 ... Domingo=6
    wd = dt.weekday()
    h, m = dt.hour, dt.minute
    t = dtime(h, m)
    if wd in (0, 1, 2, 3, 4):  # seg-sex 08:00-20:00
        return dtime(8, 0) <= t < dtime(20, 0)
    if wd == 5:  # sábado 08:00-14:00
        return dtime(8, 0) <= t < dtime(14, 0)
    return False  # domingo fechado

# ==========================
# Execução de nó
# ==========================
class Engine:
    def __init__(self, flow: Flow):
        self.flow = flow

    def step(self, session: Session, incoming_text: Optional[str]) -> Tuple[Session, List[Dict[str, Any]]]:
        out_messages: List[Dict[str, Any]] = []

        # segurança: se node inválido ou 'end', reinicia no start
        node = self.flow.nodes.get(session.node_id)
        if not node or node.type == "end":
            session.node_id = self.flow.start
            node = self.flow.nodes[session.node_id]
            session.ctx.pop("_awaiting_question", None)

        progressed = True
        while progressed:
            progressed = False
            kind = node.type
            data = node.data

            if kind == "message":
                text = render_text(data.get("text", ""), {"ctx": session.ctx, "contact": session.contact})
                if text:
                    out_messages.append({"type": "text", "text": text})
                next_id = data.get("next")
                if next_id:
                    session.node_id = next_id
                    node = self.flow.nodes[next_id]
                    progressed = True
                    # Nunca trate o incoming atual como resposta da pergunta recém-enviada
                    continue

            elif kind == "question":
                awaiting = session.ctx.get("_awaiting_question")

                # Já perguntamos antes e agora chegou a resposta
                if incoming_text is not None and awaiting == node.id:
                    save_as = data.get("save_as")
                    if save_as:
                        session.ctx[save_as] = str(incoming_text).strip()[:120]
                    session.ctx.pop("_awaiting_question", None)
                    session.node_id = data.get("next")
                    node = self.flow.nodes[session.node_id]
                    progressed = True
                    incoming_text = None  # consumiu
                    continue

                # Ainda não perguntamos (neste node): envia pergunta (se houver texto) e marca aguardando
                q_text = render_text(data.get("text", ""), {"ctx": session.ctx, "contact": session.contact})
                if q_text:
                    out_messages.append({"type": "text", "text": q_text})
                session.ctx["_awaiting_question"] = node.id
                break  # espera próxima mensagem do cliente

            elif kind == "choice":
                mapped = None
                if incoming_text is not None:
                    for opt in data.get("options", []):
                        if str(incoming_text).strip().lower() == str(opt.get("value","")).strip().lower():
                            mapped = opt.get("next"); break
                    if not mapped:
                        intent = detect_intent(incoming_text, self.flow.intents)
                        if intent:
                            mapped = data.get("intent_map", {}).get(intent)
                if mapped:
                    session.node_id = mapped
                    node = self.flow.nodes[session.node_id]
                    progressed = True
                    incoming_text = None
                    session.ctx.pop("_awaiting_question", None)
                    continue

                txt = render_text(data.get("text", ""), {"ctx": session.ctx, "contact": session.contact})
                opts = [(opt.get("label"), opt.get("value")) for opt in data.get("options", [])]
                out_messages.append({"type": "buttons", "text": txt, "options": opts})
                break  # aguarda ação do usuário

            elif kind == "action":
                act = data.get("action")

                if act == "call_webhook":
                    url = render_text(data.get("url", ""), {"ctx": session.ctx, "contact": session.contact})
                    method = (data.get("method") or "GET").upper()
                    body = data.get("body") or {}
                    rendered_body = json.loads(render_text(json.dumps(body), {"ctx": session.ctx, "contact": session.contact}))
                    try:
                        headers = {}
                        if method == "POST":
                            ok, payload, _ = _post_json(url, headers, rendered_body)
                        else:
                            ok, payload, _ = _get_json(url, headers, rendered_body)

                        # merge “higienizado” no ctx
                        _merge_ctx_from_payload(session.ctx, payload)

                        next_id = (data.get("on_success_next") or data.get("next")) if ok else (data.get("on_error_next") or data.get("fallback_next") or session.node_id)
                        session.node_id = next_id
                    except Exception:
                        session.node_id = data.get("on_error_next") or data.get("fallback_next") or session.node_id
                    node = self.flow.nodes.get(session.node_id, node)
                    progressed = True
                    session.ctx.pop("_awaiting_question", None)
                    continue

                elif act == "delay":
                    seconds = int(data.get("seconds") or 0)
                    seconds = max(0, min(seconds, 5))  # sanidade: não travar o handler
                    if seconds > 0:
                        time.sleep(seconds)
                    session.node_id = data.get("next") or session.node_id
                    node = self.flow.nodes[session.node_id]
                    progressed = True
                    session.ctx.pop("_awaiting_question", None)
                    continue

                elif act == "business_hours_gate":
                    tz = data.get("timezone") or "America/Sao_Paulo"
                    now_dt = _now_in_tz(tz)
                    in_hours = _is_business_hours(now_dt)
                    session.ctx["business_hours"] = bool(in_hours)
                    if in_hours and data.get("in_hours_next"):
                        session.node_id = data["in_hours_next"]
                    elif not in_hours and data.get("off_hours_next"):
                        session.node_id = data["off_hours_next"]
                    elif data.get("next"):
                        session.node_id = data["next"]
                    node = self.flow.nodes.get(session.node_id, node)
                    progressed = True
                    session.ctx.pop("_awaiting_question", None)
                    continue

                else:
                    # ação desconhecida → tenta next/fallback
                    session.node_id = data.get("fallback_next") or data.get("next") or session.node_id
                    node = self.flow.nodes.get(session.node_id, node)
                    progressed = True
                    session.ctx.pop("_awaiting_question", None)
                    continue

            elif kind == "handoff":
                text = render_text(data.get("text", ""), {"ctx": session.ctx, "contact": session.contact})
                if text:
                    out_messages.append({"type": "text", "text": text})
                session.assigned = "human"
                next_id = data.get("next")
                if next_id:
                    session.node_id = next_id
                    node = self.flow.nodes[next_id]
                    progressed = True
                    session.ctx.pop("_awaiting_question", None)
                    continue
                break

            elif kind == "end":
                session.ctx.pop("_awaiting_question", None)
                break

            else:
                out_messages.append({"type": "text", "text": "Desculpe, tive um imprevisto técnico. Vou te redirecionar a um atendente."})
                session.assigned = "human"
                session.ctx.pop("_awaiting_question", None)
                break

        return session, out_messages

# ==========================
# Intents (se usar choice)
# ==========================
def detect_intent(text: str, intents: Dict[str, Any]) -> Optional[str]:
    t = (text or "").lower()
    for name, spec in intents.items():
        any_terms = [s.lower() for s in spec.get("any", [])]
        if any_terms and any(any_term in t for any_term in any_terms):
            return name
        regex = spec.get("regex")
        if regex and re.search(regex, t, flags=re.I):
            return name
    return None

# ==========================
# Persistência em mensagens_avulsas
# ==========================
def _extract_msg_id(resp_json: Dict[str, Any]) -> Optional[str]:
    try:
        msgs = resp_json.get("messages")
        if isinstance(msgs, list) and msgs:
            return msgs[0].get("id")
    except Exception:
        pass
    return None

def _normalize_tel(p: str) -> str:
    d = re.sub(r"\D", "", p or "")
    if not d.startswith("55"):
        d = "55" + d
    if len(d) >= 13 and d[4] == "9":
        d = d[:4] + d[5:]
    return d

def _save_outgoing_to_avulsas(
    telefone: str,
    conteudo: str,
    phone_id: str,
    waba_id: Optional[str],
    status: str,
    msg_id: Optional[str],
    resposta_raw: Optional[Dict[str, Any]],
    nome_exibicao: str = "Agente Virtual"
) -> None:
    tel_norm = _normalize_tel(telefone)
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO mensagens_avulsas
                    (nome_exibicao, remetente, telefone_norm, conteudo, phone_id, waba_id, status, msg_id, resposta_raw)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    nome_exibicao,
                    telefone,
                    tel_norm,
                    conteudo,
                    phone_id,
                    waba_id,
                    status,
                    msg_id,
                    json.dumps(resposta_raw) if resposta_raw is not None else None,
                ),
            )
    except Exception:
        # não derruba o fluxo se o log falhar
        pass

def _merge_ctx_from_payload(ctx: Dict[str, Any], payload: Dict[str, Any]) -> None:
    """Só adiciona chaves seguras e corta strings muito grandes."""
    if not isinstance(payload, dict):
        return
    for k, v in payload.items():
        if k not in SAFE_KEYS:
            continue
        if isinstance(v, str) and len(v) > SAFE_STR_MAXLEN:
            v = v[:SAFE_STR_MAXLEN]
        ctx[k] = v

# ==========================
# Função plug-and-play para o webhook
# ==========================
def handle_incoming(
    wa_phone: str,
    incoming_text: Optional[str],
    flow_file: str,
    contact: Optional[Dict[str, Any]] = None,
    phone_id: Optional[str] = None,
    waba_id: Optional[str] = None,
    token: Optional[str] = None,
) -> None:
    # filtros opcionais por conta
    if ALLOWED_PHONE_IDS and (phone_id or PHONE_ID) not in ALLOWED_PHONE_IDS:
        Store.log(wa_phone, "err", {"error": "phone_id não permitido", "phone_id": phone_id or PHONE_ID})
        return
    if ALLOWED_WABA_IDS and (waba_id or WABA_ID) not in ALLOWED_WABA_IDS:
        Store.log(wa_phone, "err", {"error": "waba_id não permitido", "waba_id": waba_id or WABA_ID})
        return

    Store.log(wa_phone, "in", {"text": incoming_text or ""})

    # carrega flow (cacheado por mtime)
    flow = load_flow_from_file(flow_file)

    # carrega sessão com lock
    session = Store.get_session(wa_phone)
    if not session:
        session = Session(
            wa_phone=wa_phone, flow_id=flow.flow_id, node_id=flow.start,
            ctx={}, contact=contact or {}, assigned="virtual"
        )
    else:
        if session.flow_id != flow.flow_id:
            session.flow_id = flow.flow_id
            session.node_id = flow.start
            session.ctx.pop("_awaiting_question", None)
        if contact:
            session.contact.update(contact)

    # processa um passo do fluxo
    engine = Engine(flow)
    session, out_msgs = engine.step(session, incoming_text)

    pid = phone_id or PHONE_ID
    wid = waba_id or WABA_ID

    # envia as mensagens de saída
    for msg in out_msgs:
        if msg["type"] == "text":
            resp = send_wa_text(wa_phone, msg["text"], phone_id=pid, token=token)
            Store.log(wa_phone, "out", {"request": {**msg, "phone_id": pid, "waba_id": wid}, "response": resp})

            conteudo = msg["text"]
            mid = _extract_msg_id(resp)
            status = "enviado" if mid else "erro"

            _save_outgoing_to_avulsas(
                telefone=wa_phone,
                conteudo=conteudo,
                phone_id=pid or "",
                waba_id=wid or None,
                status=status,
                msg_id=mid,
                resposta_raw=resp,
                nome_exibicao="Agente Virtual",
            )

        elif msg["type"] == "buttons":
            resp = send_wa_buttons(wa_phone, msg["text"], msg["options"], phone_id=pid, token=token)
            Store.log(wa_phone, "out", {"request": {**msg, "phone_id": pid, "waba_id": wid}, "response": resp})

            opts_txt = "\n".join([f"- {label} ({value})" for label, value in msg.get("options", [])])
            conteudo = msg["text"] + (f"\n\nOpções:\n{opts_txt}" if opts_txt else "")

            mid = _extract_msg_id(resp)
            status = "enviado" if mid else "erro"

            _save_outgoing_to_avulsas(
                telefone=wa_phone,
                conteudo=conteudo,
                phone_id=pid or "",
                waba_id=wid or None,
                status=status,
                msg_id=mid,
                resposta_raw=resp,
                nome_exibicao="Agente Virtual",
            )

    # persiste sessão (idempotente)
    Store.upsert_session(session)

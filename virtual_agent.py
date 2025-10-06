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
import requests
import yaml

from datetime import datetime, time as dtime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except ImportError:
    ZoneInfo = None

BASE_DIR = Path(__file__).resolve().parent

# ==========================
# Config via ambiente
# ==========================
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"
)
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
PHONE_ID = os.getenv("PHONE_ID")
GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v23.0")
WABA_ID = os.getenv("WABA_ID", "")

# Restrição opcional por conta/phone_id (se definido, o webhook só processa esses IDs)
ALLOWED_PHONE_IDS = set(filter(None, os.getenv("ALLOWED_PHONE_IDS", "").split(",")))  # ex: "732661079928516"
ALLOWED_WABA_IDS = set(filter(None, os.getenv("ALLOWED_WABA_IDS", "").split(",")))    # ex: "1910445533050310"

# ==========================
# Conexão Postgres
# ==========================
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

# ==========================
# Auxiliares WhatsApp API
# ==========================
def send_wa_text(to: str, body: str, phone_id: Optional[str] = None, token: Optional[str] = None) -> Dict[str, Any]:
    pid = phone_id or PHONE_ID
    tok = token or WHATSAPP_TOKEN
    if not pid:
        return {"error": {"message": "PHONE_ID ausente"}}
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{pid}/messages"
    headers = {
        "Authorization": f"Bearer {tok}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"preview_url": False, "body": body[:4000]},
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    try:
        return r.json()
    finally:
        r.close()

def send_wa_buttons(to: str, body: str, options: List[Tuple[str, str]], phone_id: Optional[str] = None, token: Optional[str] = None) -> Dict[str, Any]:
    pid = phone_id or PHONE_ID
    tok = token or WHATSAPP_TOKEN
    if not pid:
        return {"error": {"message": "PHONE_ID ausente"}}
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{pid}/messages"
    headers = {
        "Authorization": f"Bearer {tok}",
        "Content-Type": "application/json",
    }
    buttons = [
        {"type": "reply", "reply": {"id": v[:256], "title": l[:20]}}
        for (l, v) in options[:3]
    ]
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body[:1024]},
            "action": {"buttons": buttons},
        },
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    try:
        return r.json()
    finally:
        r.close()

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

    @staticmethod
    def load_from_file(path: str) -> "Flow":
        p = Path(path)
        if not p.is_absolute():
            p = BASE_DIR / p
        with open(p, "r", encoding="utf-8") as f:
            spec = yaml.safe_load(f)
        return Flow(spec)

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
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM bot_sessions WHERE wa_phone=%s ORDER BY updated_at DESC LIMIT 1",
                (wa_phone,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return Session(
            wa_phone=row["wa_phone"],
            flow_id=row["flow_id"],
            node_id=row["node_id"],
            ctx=row["ctx"] or {},
            contact=row["contact"] or {},
            assigned=row["assigned"],
        )

    @staticmethod
    def upsert_session(s: Session) -> None:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_sessions (wa_phone, flow_id, node_id, ctx, contact, assigned, updated_at)
                VALUES (%s,%s,%s,%s::jsonb,%s::jsonb,%s, NOW())
                ON CONFLICT (wa_phone) DO UPDATE SET
                    flow_id = EXCLUDED.flow_id,
                    node_id = EXCLUDED.node_id,
                    ctx = EXCLUDED.ctx,
                    contact = EXCLUDED.contact,
                    assigned = EXCLUDED.assigned,
                    updated_at = NOW()
                """,
                (
                    s.wa_phone,
                    s.flow_id,
                    s.node_id,
                    json.dumps(s.ctx),
                    json.dumps(s.contact),
                    s.assigned,
                ),
            )

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
                        session.ctx[save_as] = incoming_text.strip()[:120]
                    session.ctx.pop("_awaiting_question", None)
                    session.node_id = data.get("next")
                    node = self.flow.nodes[session.node_id]
                    progressed = True
                    incoming_text = None  # consumiu
                    continue

                # Ainda não perguntamos (neste node): envia pergunta (se houver texto) e marca aguardando
                q_text = render_text(data.get("text", ""), {"ctx": session.ctx, "contact": session.contact})
                if q_text:  # só envia se houver conteúdo
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
                        if method == "POST":
                            resp = requests.post(url, json=rendered_body, timeout=30)
                        else:
                            resp = requests.get(url, params=rendered_body, timeout=30)
                        payload = resp.json() if "application/json" in resp.headers.get("Content-Type", "") else {"text": resp.text}
                        for k, v in payload.items():
                            session.ctx[k] = v
                        next_id = data.get("on_success_next") or data.get("next")
                        session.node_id = next_id
                    except Exception:
                        session.node_id = data.get("on_error_next") or data.get("fallback_next") or session.node_id
                    node = self.flow.nodes[session.node_id]
                    progressed = True
                    session.ctx.pop("_awaiting_question", None)
                    continue

                elif act == "delay":
                    seconds = int(data.get("seconds") or 0)
                    seconds = max(0, min(seconds, 30))  # sanidade
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
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO mensagens_avulsas
                    (nome_exibicao, remetente, conteudo, phone_id, waba_id, status, msg_id, resposta_raw)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    nome_exibicao,
                    telefone,
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
    Store.log(wa_phone, "in", {"text": incoming_text or ""})

    flow = Flow.load_from_file(flow_file)
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

    engine = Engine(flow)
    session, out_msgs = engine.step(session, incoming_text)

    pid = phone_id or PHONE_ID
    wid = waba_id or WABA_ID

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

    Store.upsert_session(session)

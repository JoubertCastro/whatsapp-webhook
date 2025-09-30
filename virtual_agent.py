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

# Restrição opcional por conta/phone_id (se definido, o webhook só processa esses IDs)
ALLOWED_PHONE_IDS = set(filter(None, os.getenv("ALLOWED_PHONE_IDS", "").split(",")))  # ex: "732661079928516"
ALLOWED_WABA_IDS = set(filter(None, os.getenv("ALLOWED_WABA_IDS", "").split(",")))    # ex: "1910445533050310" (nem sempre vem no webhook)

# ==========================
# Conexão Postgres
# ==========================



def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

# ==========================
# Auxiliares WhatsApp API
# ==========================

def send_wa_text(to: str, body: str) -> Dict[str, Any]:
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"preview_url": False, "body": body[:4000]},  # limite WA
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    try:
        return r.json()
    finally:
        r.close()


def send_wa_buttons(to: str, body: str, options: List[Tuple[str, str]]) -> Dict[str, Any]:
    """Envia quick replies (botões) usando message template de interactive."""
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }
    buttons = [
        {"type": "reply", "reply": {"id": v[:256], "title": l[:20]}}
        for (l, v) in options[:3]  # WA limita a 3 botões de resposta rápida
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
# Render de templates simples
# ==========================

def render_text(tpl: str, ctx: Dict[str, Any]) -> str:
    def repl(m: re.Match) -> str:
        expr = m.group(1).strip()
        # suporte a fallback: foo.bar or "alguma coisa"
        fallback = ""
        if " or " in expr:
            left, right = expr.split(" or ", 1)
            expr = left.strip()
            mfb = re.match(r'^[\'"](.*?)[\'"]$', right.strip())
            if mfb:
                fallback = mfb.group(1)
        # resolve path com pontos
        parts = expr.split(".")
        val: Any = ctx
        for p in parts:
            val = val.get(p) if isinstance(val, dict) else None
            if val is None:
                break
        return str(val) if val is not None else fallback
    return re.sub(r"\{\{([^}]+)\}\}", repl, tpl)



# ==========================
# Intents simples (palavras‑chave/regex)
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
# Execução de nó
# ==========================

class Engine:
    def __init__(self, flow: Flow):
        self.flow = flow

    def step(self, session: Session, incoming_text: Optional[str]) -> Tuple[Session, List[Dict[str, Any]]]:
        """Processa a entrada do usuário e retorna mensagens de saída.
        Pode atravessar múltiplos nós automaticamente (message->next, etc.).
        """
        out_messages: List[Dict[str, Any]] = []
        node = self.flow.nodes.get(session.node_id)
        if not node:
            # reinicia para segurança
            session.node_id = self.flow.start
            node = self.flow.nodes[session.node_id]

        progressed = True
        while progressed:
            progressed = False
            kind = node.type
            data = node.data

            if kind == "message":
                text = render_text(data.get("text", ""), {"ctx": session.ctx, "contact": session.contact})
                out_messages.append({"type": "text", "text": text})
                next_id = data.get("next")
                if next_id:
                    session.node_id = next_id
                    node = self.flow.nodes[next_id]
                    progressed = True
                    continue

            elif kind == "question":
                # Se recebemos uma resposta agora, salvar e avançar
                if incoming_text is not None:
                    save_as = data.get("save_as")
                    if save_as:
                        session.ctx[save_as] = incoming_text.strip()[:120]
                    session.node_id = data.get("next")
                    node = self.flow.nodes[session.node_id]
                    progressed = True
                    incoming_text = None  # consumiu a entrada
                    continue
                else:
                    text = render_text(data.get("text", ""), {"ctx": session.ctx, "contact": session.contact})
                    out_messages.append({"type": "text", "text": text})

            elif kind == "choice":
                mapped = None
                if incoming_text is not None:
                    # 3.1: casa pelo valor exato de uma opção
                    for opt in data.get("options", []):
                        if str(incoming_text).strip().lower() == str(opt.get("value","")).strip().lower():
                            mapped = opt.get("next")
                            break
                    # 3.2: se não casou por value, tenta por intent
                    if not mapped:
                        intent = detect_intent(incoming_text, self.flow.intents)
                        if intent:
                            mapped = data.get("intent_map", {}).get(intent)
                if mapped:
                    session.node_id = mapped
                    node = self.flow.nodes[session.node_id]
                    progressed = True
                    incoming_text = None
                    continue
                # Sem entrada ou não casou -> apresenta botões
                text = render_text(data.get("text", ""), {"ctx": session.ctx, "contact": session.contact})
                opts = [(opt.get("label"), opt.get("value")) for opt in data.get("options", [])]
                out_messages.append({"type": "buttons", "text": text, "options": opts})
                # se o usuário enviar texto livre, tratar na próxima chamada

            elif kind == "action":
                if data.get("action") == "call_webhook":
                    url = render_text(data.get("url", ""), {"ctx": session.ctx, "contact": session.contact})
                    method = (data.get("method") or "GET").upper()
                    body = data.get("body") or {}
                    # renderiza campos do body
                    rendered_body = json.loads(render_text(json.dumps(body), {"ctx": session.ctx, "contact": session.contact}))
                    try:
                        if method == "POST":
                            resp = requests.post(url, json=rendered_body, timeout=30)
                        else:
                            resp = requests.get(url, params=rendered_body, timeout=30)
                        payload = resp.json() if "application/json" in resp.headers.get("Content-Type", "") else {"text": resp.text}
                        # mescla em ctx
                        for k, v in payload.items():
                            session.ctx[k] = v
                        next_id = data.get("on_success_next") or data.get("next")
                        session.node_id = next_id
                    except Exception:
                        session.node_id = data.get("on_error_next") or data.get("fallback_next") or session.node_id
                    node = self.flow.nodes[session.node_id]
                    progressed = True
                    continue

            elif kind == "handoff":
                text = render_text(data.get("text", ""), {"ctx": session.ctx, "contact": session.contact})
                out_messages.append({"type": "text", "text": text})
                session.assigned = "human"
                next_id = data.get("next")
                if next_id:
                    session.node_id = next_id
                    node = self.flow.nodes[next_id]
                    progressed = True
                    continue

            elif kind == "end":
                # Fim do fluxo. Mantém sessão para contexto, mas não envia nada a mais.
                pass

            else:
                # tipo desconhecido -> encerra com handoff
                out_messages.append({"type": "text", "text": "Desculpe, tive um imprevisto técnico. Vou te redirecionar a um atendente."})
                session.assigned = "human"
                break

        return session, out_messages


# ==========================
# Função plug-and-play para o webhook
# ==========================

def handle_incoming(wa_phone: str, incoming_text: Optional[str], flow_file: str, contact: Optional[Dict[str, Any]] = None) -> None:
    """Chame esta função dentro do seu webhook para processar mensagens recebidas.

    Args:
        wa_phone: número do cliente (e.g. '5561999998888')
        incoming_text: texto recebido do cliente (ou None no primeiro contato)
        flow_file: caminho para o YAML do fluxo (e.g. 'flows/onboarding.yaml')
        contact: dict opcional com dados do contato (nome, cpf etc.)
    """
    Store.log(wa_phone, "in", {"text": incoming_text or ""})

    flow = Flow.load_from_file(flow_file)
    session = Store.get_session(wa_phone)
    if not session:
        session = Session(wa_phone=wa_phone, flow_id=flow.flow_id, node_id=flow.start, ctx={}, contact=contact or {}, assigned="virtual")
    else:
        # se sessão existir mas for de outro fluxo, reinicia no novo
        if session.flow_id != flow.flow_id:
            session.flow_id = flow.flow_id
            session.node_id = flow.start

        # atualiza dados de contato se fornecido
        if contact:
            session.contact.update(contact)

    engine = Engine(flow)
    session, out_msgs = engine.step(session, incoming_text)

    # envia mensagens
    for msg in out_msgs:
        if msg["type"] == "text":
            resp = send_wa_text(wa_phone, msg["text"])
            Store.log(wa_phone, "out", {"request": msg, "response": resp})
        elif msg["type"] == "buttons":
            resp = send_wa_buttons(wa_phone, msg["text"], msg["options"]) 
            Store.log(wa_phone, "out", {"request": msg, "response": resp})

    # persiste sessão
    Store.upsert_session(session)

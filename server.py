from flask import Flask, request, jsonify
from datetime import datetime, timezone, timedelta
import psycopg2
import psycopg2.extras
import json
import os

app = Flask(__name__)

# ---------- Config ----------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway")
VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "meu_token_secreto")

# ---------- Helpers ----------
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    """Cria tabelas se não existirem"""
    conn = get_conn()
    cur = conn.cursor()
    
    # tabela mensagens recebidas
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mensagens (
            id SERIAL PRIMARY KEY,
            data_hora TIMESTAMP,
            remetente TEXT,
            mensagem TEXT
        );
    """)
    
    # tabela status das mensagens enviadas
    cur.execute("""
        CREATE TABLE IF NOT EXISTS status_mensagens (
            id SERIAL PRIMARY KEY,
            data_hora TIMESTAMP,
            msg_id TEXT,
            recipient_id TEXT,
            status TEXT,
            raw JSONB
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()

def ajustar_timestamp(ts: str):
    """Converte timestamp do WhatsApp (Unix UTC) para datetime GMT-3"""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc) - timedelta(hours=3)
    except Exception:
        return datetime.now(timezone.utc) - timedelta(hours=3)

def salvar_mensagem(remetente, mensagem, timestamp=None):
    data_hora = ajustar_timestamp(timestamp) if timestamp else datetime.now(timezone.utc) - timedelta(hours=3)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO mensagens (data_hora, remetente, mensagem) VALUES (%s, %s, %s)",
        (data_hora, remetente, mensagem)
    )
    conn.commit()
    cur.close()
    conn.close()

def salvar_status(msg_id, recipient_id, status, raw, timestamp=None):
    data_hora = ajustar_timestamp(timestamp) if timestamp else datetime.now(timezone.utc) - timedelta(hours=3)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO status_mensagens (data_hora, msg_id, recipient_id, status, raw) VALUES (%s, %s, %s, %s, %s)",
        (data_hora, msg_id, recipient_id, status, json.dumps(raw))
    )
    conn.commit()
    cur.close()
    conn.close()

# Inicializa banco
init_db()

# ---------- Webhook ----------
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        if mode == "subscribe" and token == VERIFY_TOKEN:
            return challenge, 200
        return "Erro de validação", 403

    data = request.get_json(silent=True) or {}
    print("📩 Recebi:", json.dumps(data, indent=2))

    try:
        entry = data.get("entry", [])[0]
        changes = entry.get("changes", [])[0]
        value = changes.get("value", {})

        # Mensagens recebidas
        if "messages" in value:
            for msg in value.get("messages", []):
                remetente = msg.get("from", "desconhecido")
                tipo = msg.get("type")
                texto = None
                if tipo == "text":
                    texto = msg.get("text", {}).get("body")
                else:
                    texto = f"[{tipo}]"
                salvar_mensagem(remetente, texto, msg.get("timestamp"))

        # Status de mensagens enviadas
        if "statuses" in value:
            for st in value.get("statuses", []):
                msg_id = st.get("id")
                recipient_id = st.get("recipient_id")
                status = st.get("status")
                salvar_status(msg_id, recipient_id, status, st, st.get("timestamp"))

    except Exception as e:
        print("❌ Erro ao processar webhook:", e)

    return "EVENT_RECEIVED", 200

# ---------- Teste saúde ----------
@app.route("/api/saude")
def saude():
    return jsonify({"ok": True})

# ---------- Run ----------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)

from flask import Flask, request, jsonify
from datetime import datetime, timezone, timedelta
import psycopg2
import psycopg2.extras
import json
import os

# === NOVO (LOGIN) ===
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)

# ---------- Config ----------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"
)
VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "meu_token_secreto")

# ---------- Helpers ----------
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    """Cria tabelas se não existirem"""
    conn = get_conn()
    cur = conn.cursor()

    # tabela mensagens recebidas e cliques de botão
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mensagens (
            id SERIAL PRIMARY KEY,
            data_hora TIMESTAMP,
            remetente TEXT,
            mensagem TEXT,
            direcao TEXT,
            nome TEXT,
            msg_id TEXT,
            raw JSONB
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

    # === NOVO (LOGIN) === tabela de usuários
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            senha TEXT NOT NULL,
            ativo BOOLEAN NOT NULL DEFAULT TRUE,
            criado_em TIMESTAMP DEFAULT NOW()
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

def salvar_mensagem(remetente, mensagem, msg_id=None, nome=None, timestamp=None, raw=None):
    data_hora = ajustar_timestamp(timestamp) if timestamp else datetime.now(timezone.utc) - timedelta(hours=3)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO mensagens (data_hora, remetente, mensagem, direcao, nome, msg_id, raw) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (data_hora, remetente, mensagem, "in", nome, msg_id, json.dumps(raw) if raw else None)
    )
    conn.commit()
    cur.close()
    conn.close()

def salvar_status(msg_id, recipient_id, status, raw, timestamp=None):
    data_hora = ajustar_timestamp(timestamp) if timestamp else datetime.now(timezone.utc) - timedelta(hours=3)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO status_mensagens (data_hora, msg_id, recipient_id, status, raw) "
        "VALUES (%s, %s, %s, %s, %s)",
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
    print("📩 Recebi:", json.dumps(data, indent=2, ensure_ascii=False))

    try:
        entry = data.get("entry", [])[0]
        changes = entry.get("changes", [])[0]
        value = changes.get("value", {})

        # Mensagens recebidas
        for msg in value.get("messages", []):
            remetente = msg.get("from", "desconhecido")
            msg_id = msg.get("id")
            tipo = msg.get("type")
            texto = None

            if tipo == "text":
                texto = msg.get("text", {}).get("body")
            elif tipo == "button":
                botao = msg.get("button", {})
                texto = f"{botao.get('text')} (payload: {botao.get('payload')})"
            else:
                texto = f"[{tipo}]"

            nome = value.get("contacts", [{}])[0].get("profile", {}).get("name")

            salvar_mensagem(remetente, texto, msg_id=msg_id, nome=nome, timestamp=msg.get("timestamp"), raw=msg)

        # Status de mensagens enviadas
        for st in value.get("statuses", []):
            salvar_status(
                st.get("id"),
                st.get("recipient_id"),
                st.get("status"),
                st,
                st.get("timestamp")
            )

    except Exception as e:
        print("❌ Erro ao processar webhook:", e)

    return "EVENT_RECEIVED", 200

# ---------- Teste saúde ----------
@app.route("/api/saude")
def saude():
    return jsonify({"ok": True})

# === NOVO (LOGIN) === Cadastro de usuário
@app.route("/api/cadastrar", methods=["POST"])
def cadastrar():
    data = request.get_json(silent=True) or {}
    nome = (data.get("nome") or "").strip()
    email = (data.get("email") or "").strip().lower()
    senha = data.get("senha") or ""

    if not nome or not email or not senha:
        return jsonify({"ok": False, "erro": "nome, email e senha são obrigatórios"}), 400

    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("SELECT id FROM usuarios WHERE email = %s", (email,))
        if cur.fetchone():
            return jsonify({"ok": False, "erro": "email já cadastrado"}), 409

        cur.execute(
            "INSERT INTO usuarios (nome, email, senha) VALUES (%s, %s, %s) RETURNING id, criado_em",
            (nome, email, generate_password_hash(senha))
        )
        row = cur.fetchone()
        conn.commit()
        return jsonify({"ok": True, "id": row["id"], "criado_em": row["criado_em"]})
    except Exception as e:
        conn.rollback()
        print("❌ Erro /api/cadastrar:", e)
        return jsonify({"ok": False, "erro": "erro ao cadastrar usuário"}), 500
    finally:
        cur.close()
        conn.close()

# === NOVO (LOGIN) === Login
@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    senha = data.get("senha") or ""

    if not email or not senha:
        return jsonify({"ok": False, "erro": "email e senha são obrigatórios"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, nome, email, senha, ativo FROM usuarios WHERE email = %s", (email,))
        user = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not user:
        return jsonify({"ok": False, "erro": "credenciais inválidas"}), 401

    if not user["ativo"]:
        return jsonify({"ok": False, "erro": "usuário inativo"}), 403

    if not check_password_hash(user["senha"], senha):
        return jsonify({"ok": False, "erro": "credenciais inválidas"}), 401

    # Retorna dados básicos
    return jsonify({
        "ok": True,
        "user": {
            "id": user["id"],
            "nome": user["nome"],
            "email": user["email"],
            "ativo": user["ativo"]
        }
    })

# ---------- Run ----------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)

import os
import logging
from flask import Flask, request, jsonify
from datetime import datetime
import psycopg2
import psycopg2.extras

# ---------- Config ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Pega a URL do Postgres do Railway
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL não encontrada! Configure no Railway Settings > Variables")

VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "meu_token_secreto")

# ---------- DB helpers ----------
def get_conn():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"❌ Não foi possível conectar ao banco: {e}")
        raise

def init_db():
    """Cria tabela caso não exista"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mensagens (
                id SERIAL PRIMARY KEY,
                data_hora TIMESTAMP,
                remetente TEXT,
                mensagem TEXT
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ Tabela 'mensagens' pronta")
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar DB: {e}")

def salvar_mensagem(remetente, mensagem):
    """Insere mensagem no banco com logging de erros"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO mensagens (data_hora, remetente, mensagem) VALUES (%s, %s, %s)",
            (datetime.now(), remetente, mensagem)
        )
        conn.commit()
        logger.info(f"📥 Mensagem salva: {remetente} -> {mensagem}")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar_mensagem: {e}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

# Inicializa DB ao subir
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

    if request.method == "POST":
        data = request.json
        logger.info(f"📩 Recebi: {data}")

        try:
            mensagem = data["entry"][0]["changes"][0]["value"]["messages"][0]["text"]["body"]
            remetente = data["entry"][0]["changes"][0]["value"]["messages"][0]["from"]
        except Exception:
            remetente = "desconhecido"
            mensagem = str(data)

        salvar_mensagem(remetente, mensagem)
        return "EVENT_RECEIVED", 200

# ---------- Teste saúde ----------
@app.route("/api/saude")
def saude():
    return jsonify({"ok": True, "hora": datetime.now().isoformat()})

# ---------- Run ----------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"🚀 App rodando em 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=True)

from flask import Flask, request
from datetime import datetime
import psycopg2
import os

app = Flask(__name__)

# Usando a URL do Railway que você passou
DATABASE_URL = "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"

def salvar_mensagem(remetente, mensagem):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Cria tabela se não existir
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mensagens (
            id SERIAL PRIMARY KEY,
            data_hora TIMESTAMP,
            remetente TEXT,
            mensagem TEXT
        )
    """)
    conn.commit()

    # Insere a nova mensagem
    cur.execute(
        "INSERT INTO mensagens (data_hora, remetente, mensagem) VALUES (%s, %s, %s)",
        (datetime.now(), remetente, mensagem)
    )
    conn.commit()

    cur.close()
    conn.close()

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        verify_token = "meu_token_secreto"
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")

        if mode == "subscribe" and token == verify_token:
            return challenge, 200
        else:
            return "Erro de validação", 403

    if request.method == "POST":
        data = request.json
        print("📩 Recebi:", data)

        try:
            mensagem = data["entry"][0]["changes"][0]["value"]["messages"][0]["text"]["body"]
            remetente = data["entry"][0]["changes"][0]["value"]["messages"][0]["from"]
        except Exception:
            remetente = "desconhecido"
            mensagem = str(data)

        salvar_mensagem(remetente, mensagem)
        return "EVENT_RECEIVED", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)

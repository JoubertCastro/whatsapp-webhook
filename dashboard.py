from flask import Flask, jsonify, request
import psycopg2, psycopg2.extras, os

app = Flask(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"
)
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

@app.route("/api/dashboard/resumo")
def resumo():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 
              COUNT(*) total,
              COUNT(*) FILTER (WHERE status = 'sent') enviados,
              COUNT(*) FILTER (WHERE status = 'delivered') entregues,
              COUNT(*) FILTER (WHERE status = 'read') lidos
            FROM status_mensagens
        """)
        return jsonify(cur.fetchone())
    finally:
        cur.close()
        conn.close()

@app.route("/api/dashboard/envios")
def envios():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT e.id, e.nome_disparo, e.grupo_trabalho, e.criado_em,
                   COUNT(a.id) total, 
                   COUNT(*) FILTER (WHERE a.status = 'delivered') entregues,
                   COUNT(*) FILTER (WHERE a.status = 'read') lidos
            FROM envios e
            LEFT JOIN envios_analitico a ON e.id = a.envio_id
            GROUP BY e.id
            ORDER BY e.criado_em DESC
        """)
        return jsonify(cur.fetchall())
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 6000))  # roda separado do server.py
    app.run(host="0.0.0.0", port=port, debug=True)

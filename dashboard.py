from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import psycopg2, psycopg2.extras, os

app = Flask(__name__, static_folder=".", static_url_path="")
CORS(app)  # habilita CORS depois de criar o app

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"
)

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

# ---------- Endpoints API ----------
@app.route("/api/dashboard/resumo")
def resumo():
    conn = get_conn()
    cur = conn.cursor()
    try:
        # filtros opcionais
        start = request.args.get("start")  # YYYY-MM-DD
        end = request.args.get("end")      # YYYY-MM-DD
        phone_id = request.args.get("phone_id")  # ex: "732661079928516"

        where = []
        params = []

        # aqui assumo que status_mensagens tem col data_hora e phone_id (ajuste se o nome for outro)
        if start:
            where.append("data_hora::date >= %s")
            params.append(start)
        if end:
            where.append("data_hora::date <= %s")
            params.append(end)
        if phone_id:
            where.append("phone_id = %s")
            params.append(phone_id)

        sql = f"""
            SELECT 
              COUNT(*) total,
              COUNT(*) FILTER (WHERE status = 'sent') enviados,
              COUNT(*) FILTER (WHERE status = 'delivered') entregues,
              COUNT(*) FILTER (WHERE status = 'read') lidos
            FROM status_mensagens
            {"WHERE " + " AND ".join(where) if where else ""}
        """
        cur.execute(sql, tuple(params))
        return jsonify(cur.fetchone())
    finally:
        cur.close(); conn.close()


@app.route("/api/dashboard/envios")
def envios():
    conn = get_conn()
    cur = conn.cursor()
    try:
        # filtros opcionais
        start = request.args.get("start")
        end = request.args.get("end")
        phone_id = request.args.get("phone_id")
        like_nome = request.args.get("q")  # busca por nome_disparo

        where = []
        params = []

        if start:
            where.append("e.criado_em::date >= %s"); params.append(start)
        if end:
            where.append("e.criado_em::date <= %s"); params.append(end)
        if phone_id:
            where.append("a.phone_id = %s"); params.append(phone_id)
        if like_nome:
            where.append("LOWER(e.nome_disparo) LIKE %s"); params.append(f"%{like_nome.lower()}%")

        sql = f"""
            SELECT e.id, e.nome_disparo, e.grupo_trabalho, e.criado_em,
                   COUNT(a.id) total, 
                   COUNT(*) FILTER (WHERE a.status = 'delivered') entregues,
                   COUNT(*) FILTER (WHERE a.status = 'read') lidos
            FROM envios e
            LEFT JOIN envios_analitico a ON e.id = a.envio_id
            {"WHERE " + " AND ".join(where) if where else ""}
            GROUP BY e.id
            ORDER BY e.criado_em DESC
            LIMIT 100
        """
        cur.execute(sql, tuple(params))
        return jsonify(cur.fetchall())
    finally:
        cur.close(); conn.close()

# --- NOVO: resumo de atendimentos ---
@app.route("/api/atendimentos/resumo")
def atend_resumo():
    start = request.args.get("start")
    end = request.args.get("end")
    phone_id = request.args.get("phone_id")

    where_t = []
    where_b = []
    where_m = []

    params_t = []
    params_b = []
    params_m = []

    # conversas_em_andamento.started_at / ended_at
    if start:
        where_t.append("started_at::date >= %s"); params_t.append(start)
        where_b.append("bloqueado_at::date >= %s"); params_b.append(start)
        where_m.append("data_hora::date >= %s"); params_m.append(start)
    if end:
        where_t.append("started_at::date <= %s"); params_t.append(end)
        where_b.append("bloqueado_at::date <= %s"); params_b.append(end)
        where_m.append("data_hora::date <= %s"); params_m.append(end)
    if phone_id:
        where_t.append("phone_id = %s"); params_t.append(phone_id)
        where_b.append("phone_id = %s"); params_b.append(phone_id)
        where_m.append("phone_number_id = %s"); params_m.append(phone_id)

    conn = get_conn(); cur = conn.cursor()
    try:
        # em atendimento = started no período e ainda sem ended
        cur.execute(f"""
            SELECT COUNT(*) em_atendimento
            FROM conversas_em_andamento
            {"WHERE " + " AND ".join(where_t) if where_t else ""}
            AND ended_at IS NULL
        """, tuple(params_t))
        em_at = cur.fetchone()["em_atendimento"]

        # concluídos (bloqueados) no período
        cur.execute(f"""
            SELECT COUNT(*) concluidos
            FROM tickets_bloqueados
            {"WHERE " + " AND ".join(where_b) if where_b else ""}
        """, tuple(params_b))
        concl = cur.fetchone()["concluidos"]

        # tempo médio 1ª resposta (da 1ª 'in' pra 1ª 'out' subsequente no mesmo telefone/phone_id, no período)
        cur.execute(f"""
            WITH ins AS (
              SELECT telefone, phone_number_id AS phone_id, MIN(data_hora) AS first_in
              FROM mensagens
              WHERE direcao='in'
              {"AND " + " AND ".join(where_m) if where_m else ""}
              GROUP BY 1,2
            ), outs AS (
              SELECT m.telefone, m.phone_number_id AS phone_id, MIN(m.data_hora) AS first_out
              FROM mensagens m
              JOIN ins i ON i.telefone = m.telefone AND i.phone_id = m.phone_number_id
              WHERE m.direcao='out' AND m.data_hora >= i.first_in
              GROUP BY 1,2
            )
            SELECT AVG(first_out - first_in) AS tmr
            FROM ins JOIN outs USING (telefone, phone_id)
        """, tuple(params_m))
        tmr = cur.fetchone()["tmr"]  # pode ser None

        # TMA: média (ended_at - started_at) para conversas finalizadas no período
        cur.execute(f"""
            SELECT AVG(ended_at - started_at) AS tma
            FROM conversas_em_andamento
            {"WHERE " + " AND ".join(where_t) if where_t else ""}
            AND ended_at IS NOT NULL
        """, tuple(params_t))
        tma = cur.fetchone()["tma"]

        return jsonify({
            "em_atendimento": em_at or 0,
            "concluidos": concl or 0,
            "tma_segundos": int(tma.total_seconds()) if tma else None,
            "tmr_segundos": int(tmr.total_seconds()) if tmr else None
        })
    finally:
        cur.close(); conn.close()


# --- NOVO: motivos de conclusão (pizza) ---
@app.route("/api/atendimentos/motivos")
def atend_motivos():
    start = request.args.get("start")
    end = request.args.get("end")
    phone_id = request.args.get("phone_id")

    where = []; params = []
    if start: where.append("bloqueado_at::date >= %s"); params.append(start)
    if end: where.append("bloqueado_at::date <= %s"); params.append(end)
    if phone_id: where.append("phone_id = %s"); params.append(phone_id)

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"""
            SELECT motivo, COUNT(*) qtd
            FROM tickets_bloqueados
            {"WHERE " + " AND ".join(where) if where else ""}
            GROUP BY motivo
            ORDER BY qtd DESC
        """, tuple(params))
        return jsonify(cur.fetchall())
    finally:
        cur.close(); conn.close()


# --- NOVO: séries diárias de in/out/concluídos ---
@app.route("/api/atendimentos/series")
def atend_series():
    start = request.args.get("start")
    end = request.args.get("end")
    phone_id = request.args.get("phone_id")

    conn = get_conn(); cur = conn.cursor()
    try:
        # mensagens por dia
        where_m = []; params_m = []
        if start: where_m.append("data_hora::date >= %s"); params_m.append(start)
        if end: where_m.append("data_hora::date <= %s"); params_m.append(end)
        if phone_id: where_m.append("phone_number_id = %s"); params_m.append(phone_id)

        cur.execute(f"""
            SELECT data_hora::date AS dia,
                   SUM(CASE WHEN direcao='in' THEN 1 ELSE 0 END) AS inbound,
                   SUM(CASE WHEN direcao='out' THEN 1 ELSE 0 END) AS outbound
            FROM mensagens
            {"WHERE " + " AND ".join(where_m) if where_m else ""}
            GROUP BY 1
            ORDER BY 1
        """, tuple(params_m))
        msgs = cur.fetchall()

        # concluídos por dia (bloqueado_at)
        where_b = []; params_b = []
        if start: where_b.append("bloqueado_at::date >= %s"); params_b.append(start)
        if end: where_b.append("bloqueado_at::date <= %s"); params_b.append(end)
        if phone_id: where_b.append("phone_id = %s"); params_b.append(phone_id)

        cur.execute(f"""
            SELECT bloqueado_at::date AS dia, COUNT(*) concluidos
            FROM tickets_bloqueados
            {"WHERE " + " AND ".join(where_b) if where_b else ""}
            GROUP BY 1
            ORDER BY 1
        """, tuple(params_b))
        concl = cur.fetchall()

        return jsonify({"mensagens": msgs, "concluidos": concl})
    finally:
        cur.close(); conn.close()


# ---------- Página do Dashboard ----------
@app.route("/dashboard")
def dashboard_page():
    return send_from_directory(".", "dashboard.html")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 6000))
    app.run(host="0.0.0.0", port=port, debug=True)

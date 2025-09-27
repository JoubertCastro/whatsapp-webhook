from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import psycopg2, psycopg2.extras, os
from typing import Tuple, List, Any

app = Flask(__name__, static_folder=".", static_url_path="")
CORS(app)  # habilita CORS depois de criar o app

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"
)

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def parse_filters_for_sessions(args) -> Tuple[List[str], List[Any]]:
    conds, params = [], []
    start = args.get("start")
    end   = args.get("end")
    phone_id = args.get("phone_id")
    agentes_raw = args.get("agentes")  # ex: "12,45,87"
    agentes = [int(x) for x in agentes_raw.split(",") if x.strip().isdigit()] if agentes_raw else []

    if start:
        conds.append("t.started_at::date >= %s")
        params.append(start)
    if end:
        conds.append("t.started_at::date <= %s")
        params.append(end)
    if phone_id:
        conds.append("t.phone_id = %s")
        params.append(phone_id)
    if agentes:
        conds.append("t.codigo_do_agente = ANY(%s)")
        params.append(agentes)

    return conds, params

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
              COUNT(distinct left (msg_id,39)) total,
              COUNT(distinct left (msg_id,39)) FILTER (WHERE status in ('sent','delivered','read')) enviados,
              COUNT(distinct left (msg_id,39)) FILTER (WHERE status in ('delivered','read')) entregues,
              COUNT(distinct left (msg_id,39)) FILTER (WHERE status in ('read')) lidos
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
    conn = get_conn(); cur = conn.cursor()
    try:
        # ---------- EM ATENDIMENTO ----------
        conds, params = parse_filters_for_sessions(request.args)
        conds_em = conds[:] + ["t.ended_at IS NULL"]
        sql_em = "SELECT COUNT(*) AS em_atendimento FROM conversas_em_andamento t"
        if conds_em:
            sql_em += " WHERE " + " AND ".join(conds_em)
        cur.execute(sql_em, tuple(params))
        em_atendimento = cur.fetchone()["em_atendimento"]

        # ---------- CONCLUÍDOS ----------
        # filtra por período/phone_id e, se houver filtro de agente, casa o último agente da sessão
        start = request.args.get("start"); end = request.args.get("end"); phone_id = request.args.get("phone_id")
        agentes_raw = request.args.get("agentes")
        agentes = [int(x) for x in agentes_raw.split(",") if x.strip().isdigit()] if agentes_raw else []
        conds_b, params_b = [], []
        if start: conds_b.append("b.bloqueado_at::date >= %s"); params_b.append(start)
        if end:   conds_b.append("b.bloqueado_at::date <= %s"); params_b.append(end)
        if phone_id: conds_b.append("b.phone_id = %s"); params_b.append(phone_id)

        sql_conc = """
            WITH last_agent AS (
              SELECT b.telefone, b.phone_id, b.bloqueado_at,
                     (SELECT t.codigo_do_agente
                        FROM conversas_em_andamento t
                       WHERE t.telefone=b.telefone AND t.phone_id=b.phone_id AND t.ended_at IS NOT NULL
                       ORDER BY t.ended_at DESC
                       LIMIT 1) AS codigo_do_agente
              FROM tickets_bloqueados b
              {where_b}
            )
            SELECT COUNT(*) AS concluidos
            FROM last_agent
            {and_ag}
        """.format(
            where_b=("WHERE " + " AND ".join(conds_b)) if conds_b else "",
            and_ag=("WHERE codigo_do_agente = ANY(%s)") if agentes else ""
        )
        params_conc = params_b + ([agentes] if agentes else [])
        cur.execute(sql_conc, tuple(params_conc))
        concluidos = cur.fetchone()["concluidos"]

        # ---------- TMA (média em segundos) ----------
        conds_tma, params_tma = parse_filters_for_sessions(request.args)
        conds_tma += ["t.ended_at IS NOT NULL"]
        sql_tma = """
            SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (t.ended_at - t.started_at))), 0)::BIGINT AS tma_segundos
            FROM conversas_em_andamento t
            {where_tma}
        """.format(where_tma=("WHERE " + " AND ".join(conds_tma)) if conds_tma else "")
        cur.execute(sql_tma, tuple(params_tma))
        tma = cur.fetchone()["tma_segundos"]

        # ---------- TMR (1ª resposta em segundos) ----------
        # calcula por (telefone,phone_id): primeiro 'in' no período e primeiro 'out' após ele
        conds_msg, params_msg = [], []
        if start: conds_msg.append("m.data_hora::date >= %s"); params_msg.append(start)
        if end:   conds_msg.append("m.data_hora::date <= %s"); params_msg.append(end)
        if phone_id: conds_msg.append("m.phone_number_id = %s"); params_msg.append(phone_id)

        # limitar por agentes (quando houver) usando o "último agente" do par nesse período
        and_ag_filter = ""
        if agentes:
            and_ag_filter = """
              AND (
                SELECT t.codigo_do_agente
                FROM conversas_em_andamento t
                WHERE (t.telefone = m.remetente OR t.telefone = regexp_replace(m.remetente, '(?<=^55\\d{2})9', '', 'g'))
                  AND t.phone_id = m.phone_number_id
                ORDER BY t.ended_at DESC NULLS LAST, t.started_at DESC
                LIMIT 1
              ) = ANY(%s)
            """
            params_msg.append(agentes)

        sql_tmr = f"""
            WITH first_in AS (
              SELECT
                regexp_replace(m.remetente, '(?<=^55\\d{{2}})9', '', 'g') AS telefone,
                m.phone_number_id AS phone_id,
                MIN(m.data_hora) AS first_in
              FROM mensagens m
              WHERE m.direcao='in' {"AND " + " AND ".join(conds_msg) if conds_msg else ""}
              {and_ag_filter}
              GROUP BY 1,2
            ),
            first_out AS (
              SELECT f.telefone, f.phone_id, MIN(m2.data_hora) AS first_out
              FROM first_in f
              JOIN mensagens_avulsas m2
                ON (m2.remetente = f.telefone OR m2.remetente = '9'||substring(f.telefone from 1 for 2)||substring(f.telefone from 3))
               AND m2.phone_id = f.phone_id
               AND m2.status='enviado'
               AND m2.data_hora >= f.first_in
              GROUP BY 1,2
            )
            SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (o.first_out - i.first_in))), 0)::BIGINT AS tmr_segundos
            FROM first_in i
            LEFT JOIN first_out o
              ON o.telefone=i.telefone AND o.phone_id=i.phone_id
            WHERE o.first_out IS NOT NULL
        """
        cur.execute(sql_tmr, tuple(params_msg))
        tmr = cur.fetchone()["tmr_segundos"]

        return jsonify({
            "em_atendimento": em_atendimento,
            "concluidos": concluidos,
            "tma_segundos": int(tma or 0),
            "tmr_segundos": int(tmr or 0),
        })
    finally:
        cur.close(); conn.close()

# --- NOVO: motivos de conclusão (pizza) ---
@app.route("/api/atendimentos/motivos")
def atend_motivos():
    conn = get_conn(); cur = conn.cursor()
    try:
        start = request.args.get("start"); end = request.args.get("end"); phone_id = request.args.get("phone_id")
        agentes_raw = request.args.get("agentes")
        agentes = [int(x) for x in agentes_raw.split(",") if x.strip().isdigit()] if agentes_raw else []

        conds, params = [], []
        if start: conds.append("b.bloqueado_at::date >= %s"); params.append(start)
        if end:   conds.append("b.bloqueado_at::date <= %s"); params.append(end)
        if phone_id: conds.append("b.phone_id = %s"); params.append(phone_id)

        and_ag = ""
        if agentes:
            and_ag = "WHERE la.codigo_do_agente = ANY(%s)"
            params.append(agentes)

        sql = f"""
            WITH base AS (
              SELECT b.telefone, b.phone_id, b.motivo, b.bloqueado_at
              FROM tickets_bloqueados b
              {("WHERE " + " AND ".join(conds)) if conds else ""}
            ),
            la AS (
              SELECT x.telefone, x.phone_id, x.motivo,
                     (SELECT t.codigo_do_agente FROM conversas_em_andamento t
                       WHERE t.telefone=x.telefone AND t.phone_id=x.phone_id AND t.ended_at IS NOT NULL
                       ORDER BY t.ended_at DESC LIMIT 1) AS codigo_do_agente
              FROM base x
            )
            SELECT motivo, COUNT(*) AS qtd
            FROM la
            {and_ag}
            GROUP BY motivo
            ORDER BY qtd DESC, motivo ASC
        """
        cur.execute(sql, tuple(params))
        return jsonify(cur.fetchall())
    finally:
        cur.close(); conn.close()


# --- NOVO: séries diárias de in/out/concluídos ---
@app.route("/api/atendimentos/series")
def atend_series():
    conn = get_conn(); cur = conn.cursor()
    try:
        start = request.args.get("start"); end = request.args.get("end"); phone_id = request.args.get("phone_id")
        agentes_raw = request.args.get("agentes")
        agentes = [int(x) for x in agentes_raw.split(",") if x.strip().isdigit()] if agentes_raw else []

        # mensagens in/out
        conds_m, params_m = [], []
        if start: conds_m.append("m.data_hora::date >= %s"); params_m.append(start)
        if end:   conds_m.append("m.data_hora::date <= %s"); params_m.append(end)
        if phone_id: conds_m.append("m.phone_number_id = %s"); params_m.append(phone_id)

        and_ag_m = ""
        if agentes:
            and_ag_m = """
              AND (
                SELECT t.codigo_do_agente
                FROM conversas_em_andamento t
                WHERE (t.telefone = m.remetente OR t.telefone = regexp_replace(m.remetente, '(?<=^55\\d{2})9', '', 'g'))
                  AND t.phone_id = m.phone_number_id
                ORDER BY t.ended_at DESC NULLS LAST, t.started_at DESC
                LIMIT 1
              ) = ANY(%s)
            """
            params_m.append(agentes)

        sql_msg = f"""
            SELECT m.data_hora::date AS dia,
                   SUM(CASE WHEN m.direcao='in'  THEN 1 ELSE 0 END) AS inbound,
                   SUM(CASE WHEN m.direcao='enviado' THEN 1 ELSE 0 END) AS outbound
            FROM (
                select data_hora,phone_number_id, direcao
                from mensagens
                union all
                select data_hora,phone_id as phone_number_id ,status as direcao
                from mensagens_avulsas ) m
            WHERE 1=1 {"AND " + " AND ".join(conds_m) if conds_m else ""}
            {and_ag_m}
            GROUP BY m.data_hora::date
            ORDER BY dia
        """
        cur.execute(sql_msg, tuple(params_m))
        series_msg = cur.fetchall()

        # concluidos/dia
        conds_b, params_b = [], []
        if start: conds_b.append("b.bloqueado_at::date >= %s"); params_b.append(start)
        if end:   conds_b.append("b.bloqueado_at::date <= %s"); params_b.append(end)
        if phone_id: conds_b.append("b.phone_id = %s"); params_b.append(phone_id)
        and_ag_b = ""
        if agentes:
            and_ag_b = "WHERE la.codigo_do_agente = ANY(%s)"
            params_b.append(agentes)

        sql_conc = f"""
            WITH base AS (
              SELECT b.telefone, b.phone_id, b.bloqueado_at::date AS dia
              FROM tickets_bloqueados b
              {("WHERE " + " AND ".join(conds_b)) if conds_b else ""}
            ),
            la AS (
              SELECT x.telefone, x.phone_id, x.dia,
                     (SELECT t.codigo_do_agente FROM conversas_em_andamento t
                       WHERE t.telefone=x.telefone AND t.phone_id=x.phone_id AND t.ended_at IS NOT NULL
                       ORDER BY t.ended_at DESC LIMIT 1) AS codigo_do_agente
              FROM base x
            )
            SELECT dia, COUNT(*) AS concluidos
            FROM la
            {and_ag_b}
            GROUP BY dia
            ORDER BY dia
        """
        cur.execute(sql_conc, tuple(params_b))
        series_conc = cur.fetchall()

        return jsonify({"mensagens": series_msg, "concluidos": series_conc})
    finally:
        cur.close(); conn.close()

# --- NOVO: hora a hora (concluídos por hora + inbound por hora) ---
@app.route("/api/atendimentos/hora_hora")
def atend_hora_hora():
    """
    Retorna:
    {
      "concluidos": [{"hora":0,"qtd":N},...,{"hora":23,"qtd":M}],
      "inbound":    [{"hora":0,"qtd":X},...,{"hora":23,"qtd":Y}]
    }
    Filtros: start, end, phone_id, agentes, motivo (opcional; aceita CSV).
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        start = request.args.get("start"); end = request.args.get("end"); phone_id = request.args.get("phone_id")
        agentes_raw = request.args.get("agentes")
        agentes = [int(x) for x in (agentes_raw or "").split(",") if x.strip().isdigit()]
        # motivo(s) de conclusão — aceita CSV, case-insensitive
        motivos_raw = request.args.get("motivo")  # ex: "Realizou negociação,Recusou a proposta"
        motivos = [m.strip().lower() for m in motivos_raw.split(",")] if motivos_raw else []

        # ---------- Concluídos hora a hora ----------
        conds_b, params_b = [], []
        if start:   conds_b.append("b.bloqueado_at::date >= %s"); params_b.append(start)
        if end:     conds_b.append("b.bloqueado_at::date <= %s"); params_b.append(end)
        if phone_id:conds_b.append("b.phone_id = %s"); params_b.append(phone_id)
        if motivos:
            conds_b.append("LOWER(b.motivo) = ANY(%s)"); params_b.append(motivos)

        and_ag_b = ""
        if agentes:
            and_ag_b = "WHERE la.codigo_do_agente = ANY(%s)"
            params_b.append(agentes)

        sql_conc_h = f"""
            WITH base AS (
              SELECT b.telefone, b.phone_id, b.bloqueado_at, b.motivo
              FROM tickets_bloqueados b
              {("WHERE " + " AND ".join(conds_b)) if conds_b else ""}
            ),
            la AS (
              SELECT x.telefone, x.phone_id, x.bloqueado_at,
                     (SELECT t.codigo_do_agente
                        FROM conversas_em_andamento t
                       WHERE t.telefone=x.telefone AND t.phone_id=x.phone_id AND t.ended_at IS NOT NULL
                       ORDER BY t.ended_at DESC LIMIT 1) AS codigo_do_agente
              FROM base x
            ),
            f AS (
              SELECT EXTRACT(HOUR FROM bloqueado_at)::int AS hora
              FROM la
              {and_ag_b}
            )
            SELECT gs.hora, COALESCE(COUNT(f.hora),0)::bigint AS qtd
            FROM generate_series(0,23) AS gs(hora)
            LEFT JOIN f ON f.hora = gs.hora
            GROUP BY gs.hora
            ORDER BY gs.hora
        """
        cur.execute(sql_conc_h, tuple(params_b))
        concluidos = cur.fetchall()  # [{"hora":0,"qtd":...},...]

        # ---------- Inbound hora a hora ----------
        conds_m, params_m = ["m.direcao='in'"], []
        if start:   conds_m.append("m.data_hora::date >= %s"); params_m.append(start)
        if end:     conds_m.append("m.data_hora::date <= %s"); params_m.append(end)
        if phone_id:conds_m.append("m.phone_number_id = %s"); params_m.append(phone_id)

        and_ag_m = ""
        if agentes:
            and_ag_m = """
              AND (
                SELECT t.codigo_do_agente
                FROM conversas_em_andamento t
                WHERE (t.telefone = m.remetente OR t.telefone = regexp_replace(m.remetente, '(?<=^55\\d{2})9', '', 'g'))
                  AND t.phone_id = m.phone_number_id
                ORDER BY t.ended_at DESC NULLS LAST, t.started_at DESC
                LIMIT 1
              ) = ANY(%s)
            """
            params_m.append(agentes)

        sql_in_h = f"""
            WITH f AS (
              SELECT EXTRACT(HOUR FROM m.data_hora)::int AS hora
              FROM mensagens m
              WHERE {" AND ".join(conds_m)}
              {and_ag_m}
            )
            SELECT gs.hora, COALESCE(COUNT(f.hora),0)::bigint AS qtd
            FROM generate_series(0,23) AS gs(hora)
            LEFT JOIN f ON f.hora = gs.hora
            GROUP BY gs.hora
            ORDER BY gs.hora
        """
        cur.execute(sql_in_h, tuple(params_m))
        inbound = cur.fetchall()

        return jsonify({"concluidos": concluidos, "inbound": inbound})
    finally:
        cur.close(); conn.close()

@app.route("/api/agentes")
def listar_agentes():
    phone_id = request.args.get("phone_id")
    conn = get_conn(); cur = conn.cursor()
    try:
        if phone_id:
            cur.execute("""
              SELECT codigo_do_agente, nome, carteira
              FROM agentes
              WHERE carteira IN (
                SELECT nome FROM (VALUES
                  ('ConnectZap','732661079928516'),
                  ('Recovery PJ','727586317113885'),
                  ('Recovery PF','802977069563598'),
                  ('Mercado Pago Cobrança','821562937700669'),
                  ('DivZero','779797401888141'),
                  ('Arc4U','829210283602406'),
                  ('Serasa','713021321904495'),
                  ('Mercado Pago Vendas','803535039503723')
                ) AS m(nome,k)
                WHERE k = %s
              )
              ORDER BY nome NULLS LAST, codigo_do_agente
            """, (phone_id,))
        else:
            cur.execute("SELECT codigo_do_agente, nome, carteira FROM agentes ORDER BY nome NULLS LAST, codigo_do_agente")
        return jsonify(cur.fetchall())
    finally:
        cur.close(); conn.close()

# ---------- Página do Dashboard ----------
@app.route("/dashboard")
def dashboard_page():
    return send_from_directory(".", "dashboard.html")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 6000))
    app.run(host="0.0.0.0", port=port, debug=True)

from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import psycopg2, psycopg2.extras, os, requests
from datetime import datetime

app = Flask(__name__)

# --- CORS ---
# Permitir múltiplas origens (ex: GitHub Pages, localhost, etc.)
# Dica: defina na Railway -> ALLOWED_ORIGINS="https://joubertcastro.github.io,https://*.github.io,*"
origins_env = os.getenv("ALLOWED_ORIGINS", "https://joubertcastro.github.io,https://*.github.io,*")
ALLOWED_ORIGINS = [o.strip() for o in origins_env.split(",") if o.strip()]

# Usa "*" se presente; senão usa a lista explícita
cors_origins = "*" if "*" in ALLOWED_ORIGINS else ALLOWED_ORIGINS

CORS(
    app,
    resources={r"/api/*": {
        "origins": cors_origins,
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "expose_headers": ["Content-Type"]
    }},
    supports_credentials=False  # não usamos cookies
)

def _origin_allowed(origin: str) -> bool:
    if not origin:
        return False
    if "*" in ALLOWED_ORIGINS:
        return True
    if origin in ALLOWED_ORIGINS:
        return True
    # curingas simples
    if origin.endswith(".github.io") and "https://*.github.io" in ALLOWED_ORIGINS:
        return True
    return False

@app.after_request
def add_cors_headers(resp):
    # Garante CORS mesmo se algum middleware falhar
    origin = request.headers.get("Origin")
    if _origin_allowed(origin):
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Vary"] = "Origin"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp

@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        r = make_response("")
        r.status_code = 204
        return r

# favicon (evita 404 no console)
@app.route("/favicon.ico")
def favicon():
    return "", 204


# --- CONFIG DB e META ---
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"
)
DEFAULT_TOKEN = os.getenv("META_TOKEN", "")
DEFAULT_PHONE_ID = os.getenv("PHONE_ID", "")
DEFAULT_WABA_ID = os.getenv("WABA_ID", "")

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def ensure_tables():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mensagens_avulsas (
                id SERIAL PRIMARY KEY,
                data_hora TIMESTAMP DEFAULT now(),
                nome_exibicao TEXT,
                remetente TEXT,
                conteudo TEXT,
                phone_id TEXT,
                waba_id TEXT,
                status TEXT,
                msg_id TEXT
            );
        """)
        conn.commit()
    finally:
        cur.close()
        conn.close()

ensure_tables()


# 🔹 Lista contatos únicos (última mensagem por contato)
@app.route("/api/conversas/contatos", methods=["GET"])
def listar_contatos():
    conn = get_conn()
    cur = conn.cursor()
    try:
        sql = r"""
            WITH dados AS (
                SELECT ea.nome_disparo, ea.grupo_trabalho, ea.data_hora,
                       ea.telefone, ea.status, ea.conteudo,
                       phone_id, string_to_array(ea.conteudo, ',') AS vars,
                       (envios.template::json ->> 'bodyText') AS body_text
                FROM envios_analitico ea
                JOIN envios ON ea.nome_disparo = envios.nome_disparo
                  AND ea.grupo_trabalho = envios.grupo_trabalho
            ),
            enviados AS (
                SELECT d.data_hora, d.telefone, d.phone_id, d.status,
                       COALESCE(rep.txt, d.body_text) AS mensagem_final
                FROM dados d
                LEFT JOIN LATERAL (
                    WITH RECURSIVE rep(i, txt) AS (
                        SELECT 0, d.body_text
                        UNION ALL
                        SELECT i+1,
                            regexp_replace(
                                txt,
                                '\{\{' || (i+1) || '\}\}',
                                COALESCE(btrim(d.vars[i+1]), ''),
                                'g'
                            )
                        FROM rep
                        WHERE i < COALESCE(array_length(d.vars, 1), 0)
                    )
                    SELECT txt FROM rep ORDER BY i DESC LIMIT 1
                ) rep ON TRUE
            ),
            cliente_msg AS (
                SELECT data_hora, remetente AS telefone, phone_number_id AS phone_id,
                       direcao AS status, mensagem AS mensagem_final
                FROM mensagens
            ),
            conversas AS (
                SELECT data_hora,
                       regexp_replace(telefone, '(?<=^55\d{2})9', '', 'g') AS telefone,
                       phone_id, status, mensagem_final
                FROM enviados
                UNION
                SELECT data_hora, telefone, phone_id, status, mensagem_final
                FROM cliente_msg
            ),
            msg_id AS (
                SELECT remetente, msg_id
                FROM (
                    SELECT data_hora, remetente, msg_id,
                        row_number() OVER (PARTITION BY remetente ORDER BY data_hora DESC) AS indice
                    FROM mensagens
                ) t
                WHERE indice = 1
            ),
            ranked AS (
                SELECT a.telefone, a.phone_id, a.status, a.mensagem_final, a.data_hora,
                       b.msg_id,
                       row_number() OVER (PARTITION BY a.telefone ORDER BY a.data_hora DESC) AS rn
                FROM conversas a
                INNER JOIN msg_id b
                  ON a.telefone = b.remetente
                  OR a.telefone = regexp_replace(b.remetente, '(?<=^55\d{2})9', '', 'g')
            )
            SELECT r.telefone AS remetente,
                   (SELECT COALESCE(nome, r.telefone) FROM mensagens m WHERE m.remetente = r.telefone ORDER BY m.data_hora DESC LIMIT 1) AS nome_exibicao,
                   r.phone_id,
                   r.msg_id,
                   r.mensagem_final,
                   r.data_hora,
                   r.status
            FROM ranked r
            WHERE r.rn = 1
            ORDER BY r.data_hora DESC;
        """
        cur.execute(sql)
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        cur.close()
        conn.close()


# 🔎 Lista conversas (relatório)
@app.route("/api/conversas", methods=["GET"])
def listar_conversas():
    filtro_telefone = request.args.get("telefone")
    filtro_phone_id = request.args.get("phone_id")
    filtro_data = request.args.get("data")

    conn = get_conn()
    cur = conn.cursor()
    try:
        sql = r"""
                WITH dados AS (
                    SELECT 
                        ea.nome_disparo,
                        ea.grupo_trabalho,
                        ea.data_hora,
                        ea.telefone,
                        ea.status,
                        ea.conteudo,
                        phone_id,
                        string_to_array(ea.conteudo, ',') AS vars,
                        (envios.template::json ->> 'bodyText') AS body_text
                    FROM envios_analitico ea
                    JOIN envios 
                    ON ea.nome_disparo = envios.nome_disparo 
                    AND ea.grupo_trabalho = envios.grupo_trabalho
                ),
                enviados AS (
                    SELECT 
                        d.nome_disparo,
                        d.grupo_trabalho,
                        d.data_hora,
                        d.telefone,
                        d.phone_id,
                        d.status,
                        COALESCE(rep.txt, d.body_text) AS mensagem_final
                    FROM dados d
                    LEFT JOIN LATERAL (
                        WITH RECURSIVE rep(i, txt) AS (
                            SELECT 0, d.body_text
                            UNION ALL
                            SELECT i + 1,
                                regexp_replace(
                                    txt,
                                    '\{\{' || (i+1) || '\}\}',
                                    COALESCE(btrim(d.vars[i+1]), ''),
                                    'g'
                                )
                            FROM rep
                            WHERE i < COALESCE(array_length(d.vars, 1), 0)
                        )
                        SELECT txt
                        FROM rep
                        ORDER BY i DESC
                        LIMIT 1
                    ) rep ON TRUE
                ),
                cliente_msg AS (
                    SELECT 
                        data_hora,
                        remetente AS telefone,
                        phone_number_id AS phone_id,
                        direcao AS status,
                        mensagem AS mensagem_final
                    FROM mensagens
                ),
                conversas AS (
                    SELECT 
                        data_hora,
                        regexp_replace(telefone, '(?<=^55\d{2})9', '', 'g') AS telefone,
                        phone_id,
                        status,
                        mensagem_final
                    FROM enviados
                    UNION
                    SELECT data_hora, telefone, phone_id, status, mensagem_final
                    FROM cliente_msg
                ),
                msg_id AS (
                    SELECT remetente, msg_id
                    FROM (
                        SELECT data_hora, remetente, msg_id,
                            row_number() OVER (PARTITION BY remetente ORDER BY data_hora DESC) AS indice
                        FROM mensagens
                    ) t
                    WHERE indice = 1
                )
                SELECT a.data_hora,
                    a.telefone,
                    a.phone_id,
                    a.status,
                    a.mensagem_final,
                    b.msg_id
                FROM conversas a
                INNER JOIN msg_id b 
                    ON a.telefone = b.remetente
                    OR a.telefone = regexp_replace(b.remetente, '(?<=^55\d{2})9', '', 'g')
        """

        filtros = []
        params = []
        if filtro_telefone:
            filtros.append("a.telefone = %s")
            params.append(filtro_telefone)
        if filtro_phone_id:
            filtros.append("a.phone_id = %s")
            params.append(filtro_phone_id)
        if filtro_data:
            filtros.append("a.data_hora::date = %s")
            params.append(filtro_data)

        if filtros:
            sql += " WHERE " + " AND ".join(filtros)

        sql += " ORDER BY a.data_hora DESC"

        cur.execute(sql, tuple(params))
        return jsonify(cur.fetchall())
    finally:
        cur.close()
        conn.close()


# 📜 Histórico com filtro por data_inicio e data_fim
@app.route("/api/conversas/<telefone>", methods=["GET"])
def historico_conversa(telefone):
    data_inicio = request.args.get("data_inicio")
    data_fim = request.args.get("data_fim")

    conn = get_conn()
    cur = conn.cursor()
    try:
        sql = r"""
            WITH dados AS (
                SELECT 
                    ea.nome_disparo,
                    ea.grupo_trabalho,
                    ea.data_hora,
                    ea.telefone,
                    ea.status,
                    ea.conteudo,
                    phone_id,
                    string_to_array(ea.conteudo, ',') AS vars,
                    (envios.template::json ->> 'bodyText') AS body_text
                FROM envios_analitico ea
                JOIN envios 
                ON ea.nome_disparo = envios.nome_disparo 
                AND ea.grupo_trabalho = envios.grupo_trabalho
            ),
            enviados AS (
                SELECT 
                    d.nome_disparo,
                    d.grupo_trabalho,
                    d.data_hora,
                    d.telefone,
                    d.phone_id,
                    d.status,
                    COALESCE(rep.txt, d.body_text) AS mensagem_final
                FROM dados d
                LEFT JOIN LATERAL (
                    WITH RECURSIVE rep(i, txt) AS (
                        SELECT 0, d.body_text
                        UNION ALL
                        SELECT i + 1,
                            regexp_replace(
                                txt,
                                '\{\{' || (i+1) || '\}\}',
                                COALESCE(btrim(d.vars[i+1]), ''),
                                'g'
                            )
                        FROM rep
                        WHERE i < COALESCE(array_length(d.vars, 1), 0)
                    )
                    SELECT txt
                    FROM rep
                    ORDER BY i DESC
                    LIMIT 1
                ) rep ON TRUE
            ),
            cliente_msg AS (
                SELECT 
                    data_hora,
                    remetente AS telefone,
                    phone_number_id AS phone_id,
                    direcao AS status,
                    mensagem AS mensagem_final
                FROM mensagens
            ),
            conversas AS (
                SELECT 
                    data_hora,
                    regexp_replace(telefone, '(?<=^55\d{2})9', '', 'g') AS telefone,
                    phone_id,
                    status,
                    mensagem_final
                FROM enviados
                UNION
                SELECT data_hora, telefone, phone_id, status, mensagem_final
                FROM cliente_msg
            ),
            msg_id AS (
                SELECT remetente, msg_id
                FROM (
                    SELECT data_hora, remetente, msg_id,
                        row_number() OVER (PARTITION BY remetente ORDER BY data_hora DESC) AS indice
                    FROM mensagens
                ) t
                WHERE indice = 1
            )
            SELECT a.data_hora,
                a.status,
                a.mensagem_final
            FROM conversas a
            INNER JOIN msg_id b 
                ON a.telefone = b.remetente
                OR a.telefone = regexp_replace(b.remetente, '(?<=^55\d{2})9', '', 'g')
            WHERE a.telefone = %s
        """
        params = [telefone]
        if data_inicio and data_fim:
            sql += " AND a.data_hora::date BETWEEN %s AND %s"
            params.extend([data_inicio, data_fim])
        elif data_inicio:
            sql += " AND a.data_hora::date >= %s"
            params.append(data_inicio)
        elif data_fim:
            sql += " AND a.data_hora::date <= %s"
            params.append(data_fim)

        sql += " ORDER BY a.data_hora;"
        cur.execute(sql, tuple(params))
        return jsonify(cur.fetchall())
    finally:
        cur.close()
        conn.close()


# ✉️ Envia mensagem avulsa (grava em mensagens_avulsas e envia via Graph API)
@app.route("/api/conversas/<telefone>", methods=["POST"])
def enviar_mensagem(telefone):
    data = request.get_json(silent=True) or {}
    texto = (data.get("texto") or "").strip()

    token = data.get("token") or DEFAULT_TOKEN
    phone_id = data.get("phone_id") or DEFAULT_PHONE_ID
    waba_id = data.get("waba_id") or DEFAULT_WABA_ID
    msg_id = data.get("msg_id")  # pode vir do front para contexto

    if not texto:
        return jsonify({"ok": False, "erro": "texto é obrigatório"}), 400
    if not token or not phone_id:
        return jsonify({"ok": False, "erro": "token ou phone_id não configurados. Use variáveis de ambiente ou passe no body."}), 400

    url = f"https://graph.facebook.com/v23.0/{phone_id}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": telefone,
        "type": "text",
        "text": {"body": texto}
    }
    # Inclui contexto se fornecido para manter o encadeamento da conversa
    if msg_id:
        payload["context"] = {"message_id": str(msg_id)}

    headers = {"Authorization": f"Bearer {token}"}

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=12)
    except Exception as e:
        return jsonify({"ok": False, "erro": f"Falha na requisição para Graph API: {str(e)}"}), 500

    ok = r.ok
    status = "enviado" if ok else "erro"

    # tenta extrair msg_id retornado pela API
    retorno_msg_id = None
    try:
        resp_json = r.json()
        if isinstance(resp_json, dict):
            msgs = resp_json.get("messages")
            if isinstance(msgs, list) and msgs:
                retorno_msg_id = msgs[0].get("id")
    except Exception:
        retorno_msg_id = None

    # Insere no banco (mensagens_avulsas)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO mensagens_avulsas (nome_exibicao, remetente, conteudo, phone_id, waba_id, status, msg_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            f"Cliente {telefone}",
            telefone,
            texto,
            phone_id,
            waba_id,
            status,
            retorno_msg_id or msg_id
        ))
        conn.commit()
    finally:
        cur.close()
        conn.close()

    if not ok:
        try:
            err = r.json().get("error", r.text)
        except Exception:
            err = r.text
        return jsonify({"ok": False, "erro": err, "status_code": r.status_code}), r.status_code

    return jsonify({"ok": True, "resposta": r.json(), "msg_id": retorno_msg_id or msg_id})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 6000))
    app.run(host="0.0.0.0", port=port, debug=True)

from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2, psycopg2.extras, os, requests

app = Flask(__name__)
CORS(app)  # Libera CORS para todas as origens

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"
)

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

# 🔹 Lista contatos únicos
@app.route("/api/conversas/contatos", methods=["GET"])
def listar_contatos():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT
                concat(remetente,' - ',COALESCE(nome, remetente)) AS nome,
                remetente
            FROM mensagens
            ORDER BY nome
        """)
        return jsonify(cur.fetchall())
    finally:
        cur.close()
        conn.close()

# 🔎 Lista contatos com última mensagem
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
            SELECT ea.nome_disparo, ea.grupo_trabalho, ea.data_hora,
                   ea.telefone, ea.status, ea.conteudo,
                   phone_id, string_to_array(ea.conteudo, ',') AS vars,
                   (envios.template::json ->> 'bodyText') AS body_text
            FROM envios_analitico ea
            JOIN envios 
              ON ea.nome_disparo = envios.nome_disparo 
             AND ea.grupo_trabalho = envios.grupo_trabalho
        ),
        enviados AS (
            SELECT nome_disparo, grupo_trabalho, data_hora, telefone, phone_id, status,
                   (
                       SELECT COALESCE(
                           string_agg(
                               regexp_replace(body_text, '\{\{' || g.idx || '\}\}', g.val, 'g'),
                               '' ORDER BY g.idx
                           ), body_text
                       )
                       FROM (
                           SELECT generate_subscripts(vars, 1) AS idx, unnest(vars) AS val
                       ) g
                   ) AS mensagem_final
            FROM dados
        ),
        cliente_msg AS (
            SELECT data_hora, remetente AS telefone, phone_number_id AS phone_id,
                   direcao AS status, mensagem AS mensagem_final
            FROM mensagens
        ),
        conversas AS (
            SELECT data_hora, regexp_replace(telefone, '(?<=^55\d{2})9', '', 'g') as telefone,
                   phone_id, status, mensagem_final
            FROM enviados
            UNION 
            SELECT data_hora, telefone, phone_id, status, mensagem_final
            FROM cliente_msg
        ),
        msg_id as (
            SELECT remetente,msg_id
            FROM (
                SELECT data_hora,remetente,msg_id,
                       row_number() OVER(Partition by remetente order by data_hora desc) Indice
                FROM mensagens
            ) t
            WHERE indice = 1
        )
        SELECT a.data_hora,a.telefone,a.phone_id,a.status,a.mensagem_final, b.msg_id
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

# 📜 Histórico
@app.route("/api/conversas/<telefone>", methods=["GET"])
def historico_conversa(telefone):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
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
                        nome_disparo,
                        grupo_trabalho,
                        data_hora,
                        telefone,
                        phone_id,
                        status,
                        (
                            SELECT 
                                COALESCE(
                                    string_agg(
                                        regexp_replace(body_text, '\{\{' || g.idx || '\}\}', g.val, 'g'),
                                        '' ORDER BY g.idx
                                    ),
                                    body_text
                                )
                            FROM (
                                SELECT generate_subscripts(vars, 1) AS idx, unnest(vars) AS val
                            ) g
                        ) AS mensagem_final
                    FROM dados
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
                    SELECT data_hora,regexp_replace(telefone, '(?<=^55\d{2})9', '', 'g') as  telefone, phone_id, status, mensagem_final
                    FROM enviados
                    UNION 
                    SELECT data_hora, telefone, phone_id, status, mensagem_final
                    FROM cliente_msg
                ),
                msg_id as (
                select remetente,msg_id
                from (
                select data_hora,remetente,msg_id,
                row_number()Over(Partition by remetente order by data_hora desc)Indice
                from mensagens)
                where indice = 1
                )
                SELECT a.data_hora,a.status,a.mensagem_final
                FROM conversas a
                INNER JOIN msg_id b ON a.telefone = b.remetente
                                    OR a.telefone = regexp_replace(b.remetente, '(?<=^55\d{2})9', '', 'g')
                where a.telefone = %s					
                ORDER BY a.data_hora;
        """, (telefone, telefone))
        return jsonify(cur.fetchall())
    finally:
        cur.close()
        conn.close()

# ✉️ Envia mensagem
@app.route("/api/conversas/<telefone>", methods=["POST"])
def enviar_mensagem(telefone):
    data = request.get_json() or {}
    phone_id = data.get("phone_id")
    msg_id = data.get("msg_id")
    texto = data.get("texto")

    if not phone_id or not msg_id or not texto:
        return jsonify({"ok": False, "erro": "phone_id, msg_id e texto são obrigatórios"}), 400

    url = f"https://graph.facebook.com/v23.0/{phone_id}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": telefone,
        "context": {"message_id": msg_id},
        "type": "text",
        "text": {"body": texto}
    }

    headers = {"Authorization": f"Bearer {os.getenv('META_TOKEN')}"}
    r = requests.post(url, headers=headers, json=payload)

    return jsonify({"ok": r.status_code == 200, "resposta": r.json()})

# 🔁 Executa o app
if __name__ == "__main__":
    port = int(os.getenv("PORT", 6000))
    app.run(host="0.0.0.0", port=port, debug=True)

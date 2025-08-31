from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2, psycopg2.extras, os, requests

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "https://joubertcastro.github.io"}})

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
                CONCAT(remetente,' - ',COALESCE(nome)) AS nome_exibicao,
                remetente
            FROM mensagens
            ORDER BY nome_exibicao
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

                -- Substituições sequenciais com CTE recursivo (uma única mensagem_final)
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
                            -- i = 0: começa com o template original
                            SELECT 0, d.body_text
                            UNION ALL
                            -- cada iteração substitui {{i+1}} por vars[i+1]
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
                        -- pega texto após a última substituição
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
                ;

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

            -- ✅ Substituições sequenciais com CTE recursivo
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
                        -- i=0: começa com o template original
                        SELECT 0, d.body_text
                        UNION ALL
                        -- em cada passo substitui {{i+1}} por vars[i+1]
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
                    -- pega o texto após a última substituição
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
                ) x
                WHERE indice = 1
            )

            SELECT a.data_hora, a.status, a.mensagem_final
            FROM conversas a
            INNER JOIN msg_id b
                    ON a.telefone = b.remetente
                    OR a.telefone = regexp_replace(b.remetente, '(?<=^55\d{2})9', '', 'g')
            WHERE a.telefone = %s
            ORDER BY a.data_hora;

        """, (telefone,))
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

    if r.status_code != 200:
        return jsonify({
            "ok": False,
            "erro": r.json().get("error", "Erro desconhecido"),
            "status_code": r.status_code
        }), r.status_code
    return jsonify({"ok": True, "resposta": r.json()})


# 🔁 Executa o app
if __name__ == "__main__":
    port = int(os.getenv("PORT", 6000))
    app.run(host="0.0.0.0", port=port, debug=True)

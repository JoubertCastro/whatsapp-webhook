from flask import Flask, request, jsonify, make_response,Response
from flask_cors import CORS
import psycopg2, psycopg2.extras, os, requests, json
from datetime import datetime
import boto3
from botocore.client import Config
import base64
import psycopg2.errors


app = Flask(__name__)

CARTEIRA_TO_PHONE = {
    "ConnectZap": "732661079928516",
    "Recovery PJ": "727586317113885",
    "Recovery PF": "802977069563598",
    "Mercado Pago Cobrança": "821562937700669",
    "DivZero": "779797401888141",
    "Arc4U": "829210283602406",
    "Serasa": "713021321904495",
    "Mercado Pago Vendas": "803535039503723",
}


ALLOWED_MOTIVOS_CONCLUSAO = [
    "Realizou negociação",
    "Solicitou 2ª via de boleto",
    "Recusou a proposta",
    "Dúvidas gerais",
    "Cliente ficou inativo",
]

# --- CORS ---
origins_env = os.getenv("ALLOWED_ORIGINS", "https://joubertcastro.github.io,https://*.github.io,*")
ALLOWED_ORIGINS = [o.strip() for o in origins_env.split(",") if o.strip()]
cors_origins = "*" if "*" in ALLOWED_ORIGINS else ALLOWED_ORIGINS

CORS(
    app,
    resources={r"/api/*": {
        "origins": cors_origins,
        "methods": ["GET", "POST", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "expose_headers": ["Content-Type"]
    }},
    supports_credentials=False
)

def _origin_allowed(origin: str) -> bool:
    if not origin:
        return False
    if "*" in ALLOWED_ORIGINS:
        return True
    if origin in ALLOWED_ORIGINS:
        return True
    if origin.endswith(".github.io") and "https://*.github.io" in ALLOWED_ORIGINS:
        return True
    return False

@app.after_request
def add_cors_headers(resp):
    origin = request.headers.get("Origin")
    if _origin_allowed(origin):
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Vary"] = "Origin"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
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
VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "meu_token_secreto")
DEFAULT_TOKEN = os.getenv("META_TOKEN", "")
DEFAULT_PHONE_ID = os.getenv("PHONE_ID", "")
DEFAULT_WABA_ID = os.getenv("WABA_ID", "")
DEFAULT_AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
DEFAULT_AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def ensure_tables():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mensagens_avulsas (
                id SERIAL PRIMARY KEY,
                data_hora TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC'),
                nome_exibicao TEXT,
                remetente TEXT NOT NULL,
                conteudo TEXT NOT NULL,
                phone_id TEXT,
                waba_id TEXT,
                status TEXT NOT NULL,
                msg_id TEXT,
                resposta_raw JSONB
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS conversas_em_andamento (
              id SERIAL PRIMARY KEY,
              telefone TEXT NOT NULL,
              phone_id TEXT NOT NULL,
              carteira TEXT NOT NULL,
              codigo_do_agente INT NOT NULL REFERENCES agentes(codigo_do_agente),
              nome_agente TEXT NOT NULL,
              started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              ended_at TIMESTAMPTZ NULL,
              last_heartbeat TIMESTAMPTZ NULL DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_conversa_ativa
              ON conversas_em_andamento (telefone, phone_id)
              WHERE ended_at IS NULL;
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tickets_bloqueados (
            telefone      TEXT NOT NULL,
            phone_id      TEXT NOT NULL,
            bloqueado_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            motivo        TEXT NOT NULL,
            PRIMARY KEY (telefone, phone_id)
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS ix_tickets_bloqueados_bloq ON tickets_bloqueados(bloqueado_at DESC);")


        conn.commit()
    finally:
        cur.close()
        conn.close()

ensure_tables()


# -----------------------------
# CONFIGURAÇÃO S3
# -----------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "connectzap")

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    config=Config(signature_version='s3v4')
)

# -----------------------------
# ROTA PARA GERAR URL PRÉ-ASSINADA PARA UPLOAD DE PDF
# -----------------------------
import uuid

@app.route("/api/upload/pdf", methods=["POST"])
def gerar_url_presign():
    data = request.get_json(silent=True) or {}
    filename = (data.get("filename") or "").strip()

    if not filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "erro": "Somente arquivos PDF são permitidos"}), 400

    # 🔹 Gera nome único no bucket preservando o original
    unique_name = f"{uuid.uuid4().hex}_{filename}"

    try:
        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": BUCKET_NAME,
                "Key": unique_name,
                "ContentType": "application/pdf"
            },
            ExpiresIn=30  # 30 segundos
        )
    except Exception as e:
        return jsonify({"ok": False, "erro": f"Falha ao gerar URL pré-assinada: {str(e)}"}), 500

    return jsonify({
        "ok": True,
        "upload_url": presigned_url,
        "file_url": f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{unique_name}",
        "original_filename": filename
    })


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
                       direcao AS status, mensagem AS mensagem_final,msg_id
                FROM mensagens
            ),
            conversas AS (
                SELECT data_hora,
                       regexp_replace(telefone, '(?<=^55\d{2})9', '', 'g') AS telefone,
                       phone_id, status, mensagem_final,''msg_id
                FROM enviados
                UNION
                SELECT data_hora, telefone, phone_id, status, mensagem_final,msg_id
                FROM cliente_msg
                UNION
                SELECT data_hora, remetente as telefone,phone_id,status,conteudo as mensagem_final,''msg_id
				from mensagens_avulsas where status not in ('erro')
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
                       row_number() OVER (PARTITION BY a.telefone,a.phone_id ORDER BY case when status = 'in'then a.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' else a.data_hora end DESC) AS rn
                FROM conversas a
                INNER JOIN msg_id b
                  ON a.telefone = b.remetente
                  OR a.telefone = regexp_replace(b.remetente, '(?<=^55\d{2})9', '', 'g')
            )
            SELECT r.telefone AS remetente,
                   (SELECT COALESCE(nome, r.telefone) FROM mensagens m WHERE m.remetente = r.telefone ORDER BY case when status = 'in'then m.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' else m.data_hora end DESC LIMIT 1) AS nome_exibicao,
                   r.phone_id,
                   r.msg_id,
                   r.mensagem_final,
                   case when status = 'in'then r.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' else r.data_hora end as data_hora,
                   r.status
            FROM ranked r
            WHERE r.rn = 1
            ORDER BY case when status = 'in'then r.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' else r.data_hora end DESC
			;
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
                SELECT ea.nome_disparo, ea.grupo_trabalho, ea.data_hora,
                       ea.telefone, ea.status, ea.conteudo,
                       phone_id, string_to_array(ea.conteudo, ',') AS vars,
                       (envios.template::json ->> 'bodyText') AS body_text
                FROM envios_analitico ea
                JOIN envios ON ea.nome_disparo = envios.nome_disparo
                  AND ea.grupo_trabalho = envios.grupo_trabalho
            ),
            enviados AS (
                SELECT d.nome_disparo, d.grupo_trabalho, d.data_hora,
                       d.telefone, d.phone_id, d.status,
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
                       direcao AS status, mensagem AS mensagem_final,msg_id
                FROM mensagens
            ),
            conversas AS (
                 SELECT data_hora,
                       regexp_replace(telefone, '(?<=^55\d{2})9', '', 'g') AS telefone,
                       phone_id, status, mensagem_final,''msg_id
                FROM enviados
                UNION
                SELECT data_hora, telefone, phone_id, status, mensagem_final,msg_id
                FROM cliente_msg
                UNION
                SELECT data_hora, remetente as telefone,phone_id,status,conteudo as mensagem_final,''msg_id
				from mensagens_avulsas where status not in ('erro')
            ),
            tb_final as (
            SELECT case when status = 'in'then a.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' else a.data_hora end as data_hora,
              a.telefone, a.phone_id,
                   a.status, a.mensagem_final, a.msg_id
            FROM conversas a
            )
            select 
            data_hora,
            a.telefone,
            a.phone_id,
            a.status,
            a.mensagem_final,
            a.msg_id
            from tb_final a
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

# 📜 Histórico com filtro por data_inicio, data_fim e phone_id
@app.route("/api/conversas/<telefone>", methods=["GET"])
def historico_conversa(telefone):
    data_inicio = request.args.get("data_inicio")
    data_fim = request.args.get("data_fim")
    filtro_phone_id = request.args.get("phone_id")  # ✅ agora usado sempre

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
                SELECT d.nome_disparo, d.grupo_trabalho, d.data_hora,
                       d.telefone, d.phone_id, d.status,
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
                                COALESCE(btrim(d.vars[i+1]), ''), 'g'
                            )
                        FROM rep
                        WHERE i < COALESCE(array_length(d.vars, 1), 0)
                    )
                    SELECT txt FROM rep ORDER BY i DESC LIMIT 1
                ) rep ON TRUE
            ),
            cliente_msg AS (
                SELECT data_hora, remetente AS telefone, phone_number_id AS phone_id,
                       direcao AS status, mensagem AS mensagem_final, msg_id
                FROM mensagens
            ),
            conversas AS (
                 SELECT data_hora,
                       regexp_replace(telefone, '(?<=^55\d{2})9', '', 'g') AS telefone,
                       phone_id, status, mensagem_final, '' msg_id
                FROM enviados
                UNION
                SELECT data_hora, telefone, phone_id, status, mensagem_final, msg_id
                FROM cliente_msg
                UNION
                SELECT data_hora, remetente AS telefone, phone_id, status, conteudo AS mensagem_final, '' msg_id
                FROM mensagens_avulsas WHERE status NOT IN ('erro')
            ),
            tb_final AS (
                SELECT CASE
                         WHEN status = 'in'
                           THEN a.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                         ELSE a.data_hora
                       END AS data_hora,
                       a.telefone, a.phone_id,
                       a.status, a.mensagem_final, a.msg_id
                FROM conversas a
                WHERE a.telefone = %s
        """
        params = [telefone]

        # ✅ se vier phone_id, aplica filtro
        if filtro_phone_id:
            sql += " AND a.phone_id = %s"
            params.append(filtro_phone_id)

        sql += """
            )
            SELECT a.data_hora, a.telefone, a.phone_id, a.status, a.mensagem_final, a.msg_id
            FROM tb_final a
        """

        # filtros opcionais de data
        if data_inicio and data_fim:
            sql += " WHERE a.data_hora::date BETWEEN %s AND %s"
            params.extend([data_inicio, data_fim])
        elif data_inicio:
            sql += " WHERE a.data_hora::date >= %s"
            params.append(data_inicio)
        elif data_fim:
            sql += " WHERE a.data_hora::date <= %s"
            params.append(data_fim)

        sql += " ORDER BY a.data_hora;"

        cur.execute(sql, tuple(params))
        return jsonify(cur.fetchall())
    finally:
        cur.close()
        conn.close()

# --------------------------------------------------
# ✉️ Faz a leitura de imagens
# --------------------------------------------------

@app.route("/api/conversas/image/<msg_id>", methods=["GET"])
def get_image_url_by_msgid(msg_id):
    if not msg_id:
        return jsonify({"ok": False, "erro": "msg_id é obrigatório"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        # 1. Buscar image_id no banco
        cur.execute(
            "SELECT raw->'image'->>'id' AS image_id FROM mensagens WHERE msg_id = %s LIMIT 1",
            (msg_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("image_id"):
            return jsonify({"ok": False, "erro": "image_id não encontrado"}), 404

        image_id = row["image_id"]

        # 2. Buscar URL temporária no Graph
        token = DEFAULT_TOKEN
        graph_url = f"https://graph.facebook.com/v23.0/{image_id}"
        meta_resp = requests.get(graph_url, params={"access_token": token}, timeout=10)
        meta_resp.raise_for_status()
        data = meta_resp.json()

        lookaside_url = data.get("url")
        mime_type = data.get("mime_type", "image/jpeg")

        if not lookaside_url:
            return jsonify({"ok": False, "erro": "URL não encontrada no Graph"}), 500

        # 3. Baixar a imagem usando o lookaside_url + Authorization
        img_resp = requests.get(
            lookaside_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=15
        )
        img_resp.raise_for_status()

        # 4. Converter para Base64 (formato data URI)
        b64_data = base64.b64encode(img_resp.content).decode("utf-8")
        data_uri = f"data:{mime_type};base64,{b64_data}"

        # 5. Retornar JSON para o frontend
        return jsonify({"ok": True, "data_uri": data_uri})

    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --------------------------------------------------
# ✉️ Faz a reproducao de audios
# --------------------------------------------------

# --------------------------------------------------
# 🎙️ ROTA: obter áudio (em base64) a partir do msg_id
# --------------------------------------------------
@app.route("/api/conversas/audio/<msg_id>", methods=["GET"])
def get_audio_by_msgid(msg_id):
    if not msg_id:
        return jsonify({"ok": False, "erro": "msg_id é obrigatório"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        # 1. Buscar audio_id no banco
        cur.execute(
            "SELECT raw->'audio'->>'id' AS audio_id FROM mensagens WHERE msg_id = %s LIMIT 1",
            (msg_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("audio_id"):
            return jsonify({"ok": False, "erro": "audio_id não encontrado"}), 404

        audio_id = row["audio_id"]

        # 2. Buscar URL temporária no Graph
        token = DEFAULT_TOKEN
        graph_url = f"https://graph.facebook.com/v23.0/{audio_id}"
        meta_resp = requests.get(graph_url, params={"access_token": token}, timeout=10)
        meta_resp.raise_for_status()
        data = meta_resp.json()

        lookaside_url = data.get("url")
        mime_type = data.get("mime_type", "audio/ogg")  # 👈 padrão do WhatsApp é OGG/opus

        if not lookaside_url:
            return jsonify({"ok": False, "erro": "URL não encontrada no Graph"}), 500

        # 3. Baixar o áudio com token
        audio_resp = requests.get(
            lookaside_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=15
        )
        audio_resp.raise_for_status()

        # 4. Converter para Base64 (formato data URI)
        b64_data = base64.b64encode(audio_resp.content).decode("utf-8")
        data_uri = f"data:{mime_type};base64,{b64_data}"

        # 5. Retornar JSON para o frontend
        return jsonify({"ok": True, "data_uri": data_uri})

    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()
# --------------------------------------------------
#  🙏 ROTA: carregar o emoji a partir do msg_id
# --------------------------------------------------
@app.route("/api/conversas/emoji/<msg_id>", methods=["GET"])
def get_emoji_by_msgid(msg_id):
    if not msg_id:
        return jsonify({"ok": False, "erro": "msg_id é obrigatório"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        # 1. Buscar emoji no banco (campo reaction->emoji)
        cur.execute(
            "SELECT raw->'reaction'->>'emoji' AS emoji FROM mensagens WHERE msg_id = %s LIMIT 1",
            (msg_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("emoji"):
            return jsonify({"ok": False, "erro": "emoji não encontrado"}), 404

        emoji = row["emoji"]

        # 2. Retornar direto no JSON (sem base64, sem Graph)
        return jsonify({"ok": True, "emoji": emoji})

    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()


# --------------------------------------------------
# ✉️ Envia mensagem avulsa (texto ou PDF)
# --------------------------------------------------
@app.route("/api/conversas/<telefone>", methods=["POST"])
def enviar_mensagem(telefone):
    data = request.get_json(silent=True) or {}
    texto = (data.get("texto") or "").strip()

    token = data.get("token") or DEFAULT_TOKEN
    phone_id = data.get("phone_id") or DEFAULT_PHONE_ID
    waba_id = data.get("waba_id") or DEFAULT_WABA_ID
    msg_id = data.get("msg_id")  # opcional

    pdf_url = data.get("file_url")
    filename = data.get("original_filename")  # 🔹 Nome original do arquivo

    if not token or not phone_id:
        return jsonify({"ok": False, "erro": "token ou phone_id não configurados"}), 400

    url = f"https://graph.facebook.com/v23.0/{phone_id}/messages"

    if pdf_url and filename:
        # Se for PDF
        payload = {
            "messaging_product": "whatsapp",
            "to": telefone,
            "type": "document",
            "document": {
                "link": pdf_url,
                "filename": filename
            }
        }
        conteudo = f"📎 PDF: {filename}\n🔗 {pdf_url}"
    else:
        # Se for texto
        if not texto:
            return jsonify({"ok": False, "erro": "texto é obrigatório"}), 400
        payload = {
            "messaging_product": "whatsapp",
            "to": telefone,
            "type": "text",
            "text": {"body": texto}
        }
        if msg_id:
            payload["context"] = {"message_id": str(msg_id)}
        conteudo = texto

    headers = {"Authorization": f"Bearer {token}"}

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=12)
    except Exception as e:
        return jsonify({"ok": False, "erro": f"Falha na requisição para Graph API: {str(e)}"}), 500

    ok = r.ok
    status = "enviado" if ok else "erro"

    retorno_msg_id = None
    resposta_raw = None
    try:
        resposta_raw = r.json()
        if isinstance(resposta_raw, dict):
            msgs = resposta_raw.get("messages")
            if isinstance(msgs, list) and msgs:
                retorno_msg_id = msgs[0].get("id")
    except Exception:
        resposta_raw = {"erro": "não foi possível parsear resposta"}

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO mensagens_avulsas
                (nome_exibicao, remetente, conteudo, phone_id, waba_id, status, msg_id, resposta_raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            f"Cliente {telefone}",
            telefone,
            conteudo,
            phone_id,
            waba_id,
            status,
            retorno_msg_id or msg_id,
            json.dumps(resposta_raw) if resposta_raw else None
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Falha ao salvar no banco: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()

    if not ok:
        return jsonify({"ok": False, "erro": resposta_raw, "status_code": r.status_code}), r.status_code

    return jsonify({"ok": True, "resposta": resposta_raw, "msg_id": retorno_msg_id or msg_id})

@app.route("/api/tickets/claim", methods=["POST"])
def tickets_claim():
    data = request.get_json(silent=True) or {}
    codigo   = data.get("codigo_do_agente")
    carteira = data.get("carteira")

    if not isinstance(codigo, int) or not carteira:
        return jsonify({"ok": False, "erro": "codigo_do_agente (int) e carteira são obrigatórios"}), 400

    phone_id = CARTEIRA_TO_PHONE.get(carteira)
    if not phone_id:
        return jsonify({"ok": False, "erro": "carteira desconhecida"}), 400

    conn = get_conn(); cur = conn.cursor()
    try:
        # 1) precisa estar online na carteira
        cur.execute("""
            SELECT 1 FROM fila_de_atendimento
            WHERE codigo_do_agente=%s AND carteira=%s
        """, (codigo, carteira))
        if not cur.fetchone():
            return jsonify({"ok": False, "erro": "agente está offline nesta carteira"}), 409

        # 2) “faxina”: libera conversas ativas de agentes que já saíram da fila
        cur.execute("""
            UPDATE conversas_em_andamento t
               SET ended_at = NOW()
             WHERE ended_at IS NULL
               AND NOT EXISTS (
                     SELECT 1 FROM fila_de_atendimento f
                      WHERE f.codigo_do_agente = t.codigo_do_agente
                        AND f.carteira = t.carteira
               )
        """)
        conn.commit()

        # 3) lista candidatos desta carteira (mesma lógica do /api/conversas/contatos com filtro)
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
                       direcao AS status, mensagem AS mensagem_final, msg_id
                FROM mensagens
            ),
            conversas AS (
                SELECT data_hora,
                       regexp_replace(telefone, '(?<=^55\\d{2})9', '', 'g') AS telefone,
                       phone_id, status, mensagem_final, ''::text AS msg_id
                FROM enviados
                UNION
                SELECT data_hora, telefone, phone_id, status, mensagem_final, msg_id
                FROM cliente_msg
                UNION
                SELECT data_hora, remetente AS telefone, phone_id, status, conteudo AS mensagem_final, ''::text AS msg_id
                FROM mensagens_avulsas WHERE status <> 'erro'
            ),
            msg_id AS (
                SELECT remetente, msg_id FROM (
                    SELECT data_hora, remetente, msg_id,
                           row_number() OVER (PARTITION BY remetente ORDER BY data_hora DESC) AS idx
                    FROM mensagens
                ) t WHERE idx = 1
            ),
            ranked AS (
                SELECT a.telefone, a.phone_id, a.status, a.mensagem_final, a.data_hora,
                       b.msg_id,
                       row_number() OVER (
                          PARTITION BY a.telefone,a.phone_id
                          ORDER BY CASE WHEN a.status='in'
                                        THEN a.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                                        ELSE a.data_hora END DESC
                       ) AS rn
                FROM conversas a
                INNER JOIN msg_id b
                  ON a.telefone = b.remetente
                  OR a.telefone = regexp_replace(b.remetente, '(?<=^55\\d{2})9', '', 'g')
            ),last_in AS (
                SELECT
                  regexp_replace(remetente, '(?<=^55\\d{2})9', '', 'g') AS telefone,
                  phone_number_id AS phone_id,
                  MAX(CASE WHEN direcao='in'
                           THEN data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                           ELSE NULL END) AS last_in
                FROM mensagens
                GROUP BY 1,2
            )
            SELECT r.telefone AS remetente,
                   (SELECT COALESCE(nome, r.telefone) FROM mensagens m
                     WHERE m.remetente = r.telefone
                     ORDER BY CASE WHEN r.status='in'
                                   THEN m.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                                   ELSE m.data_hora END DESC
                     LIMIT 1) AS nome_exibicao,
                   r.phone_id,
                   r.mensagem_final,
                   (CASE WHEN r.status='in'
                         THEN r.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                         ELSE r.data_hora END) AS data_hora,
                   r.status
            FROM ranked r
            LEFT JOIN tickets_bloqueados tb
              ON tb.telefone = r.telefone AND tb.phone_id = r.phone_id
            LEFT JOIN last_in li
              ON li.telefone = r.telefone AND li.phone_id = r.phone_id
            WHERE r.rn = 1
              AND r.phone_id = %s
              AND (CASE WHEN r.status='in'
                        THEN r.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                        ELSE r.data_hora END) >= now() - interval '1 day'
              AND (tb.telefone IS NULL OR (li.last_in IS NOT NULL AND li.last_in > tb.bloqueado_at))
            ORDER BY (r.status='in') DESC, data_hora DESC
        """
        cur.execute(sql, (phone_id,))
        candidatos = cur.fetchall()

        # 4) tenta "reservar" um candidato de cada vez
        for c in candidatos:
            try:
                cur.execute("""
                    INSERT INTO conversas_em_andamento
                    (telefone, phone_id, carteira, codigo_do_agente, nome_agente)
                    VALUES
                    (%s, %s, %s, %s, (SELECT nome FROM agentes WHERE codigo_do_agente=%s))
                    ON CONFLICT (telefone, phone_id) WHERE ended_at IS NULL DO NOTHING
                    RETURNING telefone
                """, (c["remetente"], phone_id, carteira, codigo, codigo))
                row = cur.fetchone()
                if row:
                    conn.commit()
                    return jsonify({
                        "ok": True,
                        "ticket": {
                            "remetente": c["remetente"],
                            "phone_id": phone_id,
                            "nome_exibicao": c["nome_exibicao"] or c["remetente"],
                            "mensagem_final": c["mensagem_final"],
                            "data_hora": c["data_hora"].isoformat() if c["data_hora"] else None
                        }
                    })
                conn.rollback()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                continue

        return jsonify({"ok": False, "erro": "Sem conversas disponíveis nesta carteira"}), 404

    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"claim falhou: {str(e)}"}), 500
    finally:
        cur.close(); conn.close()
@app.route("/api/tickets/minhas", methods=["GET"])
def tickets_minhas():
    try:
        codigo = int(request.args.get("agente", "0"))
    except:
        codigo = 0
    carteira = request.args.get("carteira")

    if not codigo:
        return jsonify({"ok": False, "erro": "parâmetro agente é obrigatório"}), 400

    conn = get_conn(); cur = conn.cursor()
    try:
        sql = r"""
            /* === visão de contatos (mesma do /api/conversas/contatos) === */
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
                               regexp_replace(txt, '\{\{'||(i+1)||'\}\}', COALESCE(btrim(d.vars[i+1]), ''), 'g')
                        FROM rep
                        WHERE i < COALESCE(array_length(d.vars, 1), 0)
                    )
                    SELECT txt FROM rep ORDER BY i DESC LIMIT 1
                ) rep ON TRUE
            ),
            cliente_msg AS (
                SELECT data_hora, remetente AS telefone, phone_number_id AS phone_id,
                       direcao AS status, mensagem AS mensagem_final, msg_id
                FROM mensagens
            ),
            conversas AS (
                SELECT data_hora,
                       regexp_replace(telefone, '(?<=^55\\d{2})9', '', 'g') AS telefone,
                       phone_id, status, mensagem_final, ''::text AS msg_id
                FROM enviados
                UNION
                SELECT data_hora, telefone, phone_id, status, mensagem_final, msg_id
                FROM cliente_msg
                UNION
                SELECT data_hora, remetente AS telefone, phone_id, status, conteudo AS mensagem_final, ''::text AS msg_id
                FROM mensagens_avulsas WHERE status <> 'erro'
            ),
            msg_id AS (
                SELECT remetente, msg_id FROM (
                    SELECT data_hora, remetente, msg_id,
                           row_number() OVER (PARTITION BY remetente ORDER BY data_hora DESC) AS idx
                    FROM mensagens
                ) t WHERE idx = 1
            ),
            ranked AS (
                SELECT a.telefone, a.phone_id, a.status, a.mensagem_final, a.data_hora,
                       b.msg_id,
                       row_number() OVER (
                           PARTITION BY a.telefone,a.phone_id
                           ORDER BY CASE WHEN a.status='in'
                                         THEN a.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                                         ELSE a.data_hora END DESC
                       ) AS rn
                FROM conversas a
                INNER JOIN msg_id b
                        ON a.telefone = b.remetente
                        OR a.telefone = regexp_replace(b.remetente, '(?<=^55\\d{2})9', '', 'g')
            )
            SELECT r.telefone AS remetente,
                   (SELECT COALESCE(nome, r.telefone)
                      FROM mensagens m
                      WHERE m.remetente = r.telefone
                      ORDER BY CASE WHEN r.status='in'
                                    THEN m.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                                    ELSE m.data_hora END DESC
                      LIMIT 1) AS nome_exibicao,
                   r.phone_id,
                   r.msg_id,
                   r.mensagem_final,
                   (CASE WHEN r.status='in'
                         THEN r.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                         ELSE r.data_hora END) AS data_hora,
                   r.status
            FROM ranked r
            JOIN conversas_em_andamento t
              ON t.telefone = r.telefone AND t.phone_id = r.phone_id
            WHERE r.rn = 1
              AND t.codigo_do_agente = %s
              AND t.ended_at IS NULL
              AND (%s IS NULL OR t.carteira = %s)
            ORDER BY data_hora DESC;
        """
        cur.execute(sql, (codigo, carteira, carteira))
        rows = cur.fetchall()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"ok": False, "erro": f"minhas falhou: {str(e)}"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/tickets/liberar", methods=["DELETE"])
def tickets_liberar():
    data = request.get_json(silent=True) or {}
    try:
        codigo  = int(data.get("codigo_do_agente"))
    except:
        return jsonify({"ok": False, "erro": "codigo_do_agente inválido"}), 400
    telefone = data.get("remetente")
    phone_id = data.get("phone_id")

    if not telefone or not phone_id:
        return jsonify({"ok": False, "erro": "remetente e phone_id são obrigatórios"}), 400

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE conversas_em_andamento
               SET ended_at = NOW()
             WHERE codigo_do_agente = %s
               AND telefone = %s
               AND phone_id = %s
               AND ended_at IS NULL
             RETURNING id
        """, (codigo, telefone, phone_id))
        row = cur.fetchone(); conn.commit()
        if not row:
            return jsonify({"ok": False, "erro": "ticket não encontrado ou já liberado"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"liberar falhou: {str(e)}"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/tickets/concluir", methods=["POST"])
def tickets_concluir():
    data = request.get_json(silent=True) or {}
    try:
        codigo  = int(data.get("codigo_do_agente"))
    except:
        return jsonify({"ok": False, "erro": "codigo_do_agente inválido"}), 400

    telefone = data.get("remetente")
    phone_id = data.get("phone_id")
    motivo   = (data.get("motivo") or "").strip()

    if not telefone or not phone_id:
        return jsonify({"ok": False, "erro": "remetente e phone_id são obrigatórios"}), 400

    if motivo not in ALLOWED_MOTIVOS_CONCLUSAO:
        return jsonify({"ok": False, "erro": "motivo inválido"}), 400

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE conversas_em_andamento
               SET ended_at = NOW()
             WHERE codigo_do_agente = %s
               AND telefone = %s
               AND phone_id = %s
               AND ended_at IS NULL
        """, (codigo, telefone, phone_id))

        cur.execute("""
        INSERT INTO tickets_bloqueados (telefone, phone_id, bloqueado_at, motivo)
        VALUES (%s, %s, NOW(), %s)
        ON CONFLICT (telefone, phone_id)
        DO UPDATE SET bloqueado_at = EXCLUDED.bloqueado_at, motivo = EXCLUDED.motivo
        """, (telefone, phone_id, motivo))


        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"concluir falhou: {str(e)}"}), 500
    finally:
        cur.close(); conn.close()


if __name__ == "__main__":
    port = int(os.getenv("PORT", 6000))
    app.run(host="0.0.0.0", port=port, debug=True)

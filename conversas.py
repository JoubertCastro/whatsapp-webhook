from flask import Flask, request, jsonify, make_response, Response
from flask_cors import CORS
import psycopg2, psycopg2.extras, os, requests, json
from datetime import datetime
import boto3
from botocore.client import Config
import base64
import psycopg2.errors
import re
import uuid
from typing import Optional, Tuple, List, Dict, Any

app = Flask(__name__)


# Raw mapping (as provided), including possible keys with suffixes like "PF2"
CARTEIRA_TO_PHONE_RAW = {

    "ConnectZap": "828473960349364",
    "Recovery PJ": "727586317113885",
    "Recovery PF": "864779140046932",
    "Recovery PF": "802977069563598",
    "Mercado Pago Cobrança": "873637622491517",
    "Mercado Pago Cobrança": "821562937700669",
    "DivZero": "779797401888141",
    "Arc4U": "829210283602406",
    "Serasa": "713021321904495",
    "Mercado Pago Vendas": "803535039503723",
    "Banco PAN": "805610009301153",
}

def _normalize_carteira_key(k: str) -> str:
    k = (k or "").strip()
    # collapse keys that have numeric suffixes like "Recovery PF2" -> "Recovery PF"
    return re.sub(r"\d+$", "", k).strip()

# Build aggregated mapping: carteira -> list of phone_ids
from collections import defaultdict
CARTEIRA_TO_PHONE_IDS = defaultdict(list)
for nome, pid in CARTEIRA_TO_PHONE_RAW.items():
    base = _normalize_carteira_key(nome)
    CARTEIRA_TO_PHONE_IDS[base].append(str(pid))

# Freeze as normal dict
CARTEIRA_TO_PHONE_IDS = dict(CARTEIRA_TO_PHONE_IDS)

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
        "methods": ["GET", "POST", "DELETE", "PUT", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "expose_headers": ["Content-Type", "Content-Disposition"]  # <- expõe Content-Disposition
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
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, PUT, OPTIONS"
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

VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "")

# ====== TOKENS E ROTEAMENTO POR PHONE_ID ======
PRIMARY_TOKEN = os.getenv("META_TOKEN", "")
SECONDARY_TOKEN = os.getenv("META_TOKEN_2", "")

# Estes phone_ids usam META_TOKEN (PRIMARY_TOKEN).
PRIMARY_PHONE_IDS = {
    "864779140046932",
    "873637622491517",
    "828473960349364",
}

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

#s3_client = boto3.client(
#    "s3",
#    region_name=AWS_REGION,
#    aws_access_key_id=AWS_ACCESS_KEY_ID,
#    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#    config=Config(signature_version='s3v4')
#)

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=f"https://s3.{AWS_REGION}.amazonaws.com",
    config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"})
)


# ===========================
# HELPERS DE TOKEN/GRAPH
# ===========================
def pick_token_for_phone(phone_id: str, explicit_token: Optional[str] = None) -> str:
    """
    - Se 'explicit_token' foi enviado no body -> usa ele.
    - Caso contrário:
        * phone_id em PRIMARY_PHONE_IDS -> PRIMARY_TOKEN
        * senão -> SECONDARY_TOKEN
    """
    if explicit_token:
        return explicit_token
    return PRIMARY_TOKEN if phone_id in PRIMARY_PHONE_IDS else SECONDARY_TOKEN

def graph_get(url: str, phone_id: str, params: Optional[Dict[str, Any]] = None, timeout: int = 12,
              explicit_token: Optional[str] = None) -> Tuple[Dict[str, Any], str]:
    tok = pick_token_for_phone(phone_id, explicit_token)
    q = dict(params or {})
    q["access_token"] = tok
    r = requests.get(url, params=q, timeout=timeout)
    r.raise_for_status()
    return r.json(), tok

def graph_post_messages(phone_id: str, payload: Dict[str, Any], timeout: int = 12,
                        explicit_token: Optional[str] = None) -> Tuple[requests.Response, str]:
    tok = pick_token_for_phone(phone_id, explicit_token)
    url = f"https://graph.facebook.com/v24.0/{phone_id}/messages"
    r = requests.post(url, headers={"Authorization": f"Bearer {tok}"}, json=payload, timeout=timeout)
    return r, tok

# -----------------------------
# ROTA: gerar URL PRÉ-ASSINADA PARA UPLOAD DE PDF
# -----------------------------
@app.route("/api/upload/pdf", methods=["POST"])
def gerar_url_presign():
    data = request.get_json(silent=True) or {}
    filename = (data.get("filename") or "").strip()
    if not filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "erro": "Somente arquivos PDF são permitidos"}), 400

    unique_name = f"{uuid.uuid4().hex}_{filename}"
    try:
        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": BUCKET_NAME,
                "Key": unique_name,
                "ContentType": "application/pdf",
            },
            ExpiresIn=300,  # um pouco mais de tempo
        )
    except Exception as e:
        return jsonify({"ok": False, "erro": f"Falha ao gerar URL pré-assinada: {str(e)}"}), 500

    # sempre devolva o file_url regional também
    file_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{unique_name}"
    return jsonify({"ok": True, "upload_url": presigned_url, "file_url": file_url, "original_filename": filename})

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
                SELECT data_hora, remetente AS telefone,telefone_norm, phone_number_id AS phone_id,
                       direcao AS status, mensagem AS mensagem_final,msg_id
                FROM mensagens
            ),
            conversas AS (
                SELECT data_hora,telefone,
                       regexp_replace(telefone, '(?<=^55\d{2})9', '', 'g') AS telefone_norm,
                       phone_id, status, mensagem_final,''msg_id
                FROM enviados
                UNION
                SELECT data_hora, telefone,telefone_norm, phone_id, status, mensagem_final,msg_id
                FROM cliente_msg
                UNION
                SELECT data_hora, remetente as telefone,telefone_norm,phone_id,status,conteudo as mensagem_final,''msg_id
                from mensagens_avulsas where status not in ('erro')
                            ),
            msg_id AS (
                SELECT remetente,telefone_norm, msg_id
                FROM (
                    SELECT data_hora, remetente,telefone_norm, msg_id,
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
                  OR a.telefone = b.telefone_norm
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
                       regexp_replace(telefone, '(?<=^55\\d{2})9', '', 'g') AS telefone,
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
    filtro_phone_id = request.args.get("phone_id")

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
                       regexp_replace(telefone, '(?<=^55\\d{2})9', '', 'g') AS telefone,
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
        if filtro_phone_id:
            sql += " AND a.phone_id = %s"
            params.append(filtro_phone_id)

        sql += """
            )
            SELECT a.data_hora, a.telefone, a.phone_id, a.status, a.mensagem_final, a.msg_id
            FROM tb_final a
        """

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
# ✉️ Faz a leitura de imagens (base64)
# --------------------------------------------------
@app.route("/api/conversas/image/<msg_id>", methods=["GET"])
def get_image_url_by_msgid(msg_id):
    if not msg_id:
        return jsonify({"ok": False, "erro": "msg_id é obrigatório"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        # Agora também pegamos o phone_number_id para rotear o token corretamente
        cur.execute(
            "SELECT raw->'image'->>'id' AS image_id, phone_number_id AS phone_id FROM mensagens WHERE msg_id = %s LIMIT 1",
            (msg_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("image_id"):
            return jsonify({"ok": False, "erro": "image_id não encontrado"}), 404

        image_id = row["image_id"]
        phone_id = row.get("phone_id") or ""

        graph_url = f"https://graph.facebook.com/v24.0/{image_id}"
        data, used_token = graph_get(graph_url, phone_id, timeout=10)

        lookaside_url = data.get("url")
        mime_type = data.get("mime_type", "image/jpeg")
        if not lookaside_url:
            return jsonify({"ok": False, "erro": "URL não encontrada no Graph"}), 500

        img_resp = requests.get(
            lookaside_url,
            headers={"Authorization": f"Bearer {used_token}"},
            timeout=15
        )
        img_resp.raise_for_status()

        b64_data = base64.b64encode(img_resp.content).decode("utf-8")
        data_uri = f"data:{mime_type};base64,{b64_data}"
        return jsonify({"ok": True, "data_uri": data_uri})

    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --------------------------------------------------
# 🖼️ Download/stream da imagem (Plano B)
# --------------------------------------------------
@app.route("/api/conversas/image/<msg_id>/download", methods=["GET"])
def download_image_by_msgid(msg_id):
    if not msg_id:
        return jsonify({"ok": False, "erro": "msg_id é obrigatório"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT raw->'image'->>'id' AS image_id, phone_number_id AS phone_id FROM mensagens WHERE msg_id = %s LIMIT 1",
            (msg_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("image_id"):
            return jsonify({"ok": False, "erro": "image_id não encontrado"}), 404

        image_id = row["image_id"]
        phone_id = row.get("phone_id") or ""

        graph_url = f"https://graph.facebook.com/v24.0/{image_id}"
        data, used_token = graph_get(graph_url, phone_id, timeout=10)

        lookaside_url = data.get("url")
        mime_type = data.get("mime_type", "image/jpeg")
        if not lookaside_url:
            return jsonify({"ok": False, "erro": "URL não encontrada no Graph"}), 500

        img_resp = requests.get(
            lookaside_url,
            headers={"Authorization": f"Bearer {used_token}"},
            stream=True,
            timeout=30
        )
        img_resp.raise_for_status()

        return Response(
            img_resp.iter_content(chunk_size=8192),
            content_type=mime_type,
            headers={
                "Content-Disposition": 'inline; filename="imagem.jpg"',
                "Cache-Control": "no-store"
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --------------------------------------------------
# 🎙️ ROTA: obter áudio (base64) a partir do msg_id
# --------------------------------------------------
@app.route("/api/conversas/audio/<msg_id>", methods=["GET"])
def get_audio_by_msgid(msg_id):
    if not msg_id:
        return jsonify({"ok": False, "erro": "msg_id é obrigatório"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT raw->'audio'->>'id' AS audio_id, phone_number_id AS phone_id FROM mensagens WHERE msg_id = %s LIMIT 1",
            (msg_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("audio_id"):
            return jsonify({"ok": False, "erro": "audio_id não encontrado"}), 404

        audio_id = row["audio_id"]
        phone_id = row.get("phone_id") or ""

        graph_url = f"https://graph.facebook.com/v24.0/{audio_id}"
        data, used_token = graph_get(graph_url, phone_id, timeout=10)

        lookaside_url = data.get("url")
        mime_type = data.get("mime_type", "audio/ogg")
        if not lookaside_url:
            return jsonify({"ok": False, "erro": "URL não encontrada no Graph"}), 500

        audio_resp = requests.get(
            lookaside_url,
            headers={"Authorization": f"Bearer {used_token}"},
            timeout=15
        )
        audio_resp.raise_for_status()

        b64_data = base64.b64encode(audio_resp.content).decode("utf-8")
        data_uri = f"data:{mime_type};base64,{b64_data}"
        return jsonify({"ok": True, "data_uri": data_uri})

    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --------------------------------------------------
# 🔊 Download/stream do áudio (Plano B)
# --------------------------------------------------
@app.route("/api/conversas/audio/<msg_id>/download", methods=["GET"])
def download_audio_by_msgid(msg_id):
    if not msg_id:
        return jsonify({"ok": False, "erro": "msg_id é obrigatório"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT raw->'audio'->>'id' AS audio_id, phone_number_id AS phone_id FROM mensagens WHERE msg_id = %s LIMIT 1",
            (msg_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("audio_id"):
            return jsonify({"ok": False, "erro": "audio_id não encontrado"}), 404

        audio_id = row["audio_id"]
        phone_id = row.get("phone_id") or ""

        graph_url = f"https://graph.facebook.com/v24.0/{audio_id}"
        data, used_token = graph_get(graph_url, phone_id, timeout=10)

        lookaside_url = data.get("url")
        mime_type = data.get("mime_type", "audio/ogg")
        if not lookaside_url:
            return jsonify({"ok": False, "erro": "URL não encontrada no Graph"}), 500

        audio_resp = requests.get(
            lookaside_url,
            headers={"Authorization": f"Bearer {used_token}"},
            stream=True,
            timeout=30
        )
        audio_resp.raise_for_status()

        return Response(
            audio_resp.iter_content(chunk_size=8192),
            content_type=mime_type,
            headers={
                "Content-Disposition": 'inline; filename="audio.ogg"',
                "Cache-Control": "no-store"
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --------------------------------------------------
# ⚙️ ROTA: carregar metadados de mensagens "system" por msg_id
# --------------------------------------------------
@app.route("/api/conversas/system/<msg_id>", methods=["GET"])
def get_system_by_msgid(msg_id):
    if not msg_id:
        return jsonify({"ok": False, "erro": "msg_id é obrigatório"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
              remetente,
              phone_number_id AS phone_id,
              data_hora,
              raw->'system'->>'type'   AS system_type,
              raw->'system'->>'body'   AS body,
              raw->'system'->>'wa_id'  AS new_wa_id
            FROM mensagens
            WHERE msg_id = %s
            LIMIT 1
            """,
            (msg_id,)
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"ok": False, "erro": "msg_id não encontrado"}), 404

        if not (row.get("system_type") or row.get("body") or row.get("new_wa_id")):
            cur.execute("SELECT raw->>'type' AS msg_type FROM mensagens WHERE msg_id = %s", (msg_id,))
            trow = cur.fetchone()
            if not trow or (trow.get("msg_type") or "").lower() != "system":
                return jsonify({"ok": False, "erro": "mensagem não é do tipo 'system'"}), 404

        old_wa_id = row.get("remetente")
        resp = {
            "ok": True,
            "type": row.get("system_type") or "system",
            "body": row.get("body"),
            "wa_id": row.get("new_wa_id"),
            "old_wa_id": old_wa_id,
            "from": old_wa_id,
            "phone_id": row.get("phone_id"),
            "data_hora": row["data_hora"].isoformat() if row.get("data_hora") else None,
        }

        return jsonify(resp)

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
        cur.execute(
            "SELECT raw->'reaction'->>'emoji' AS emoji FROM mensagens WHERE msg_id = %s LIMIT 1",
            (msg_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("emoji"):
            return jsonify({"ok": False, "erro": "emoji não encontrado"}), 404

        emoji = row["emoji"]
        return jsonify({"ok": True, "emoji": emoji})

    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --------------------------------------------------
# 📄 ROTA: obter documento (PDF/DOC/etc) a partir do msg_id
# --------------------------------------------------
@app.route("/api/conversas/document/<msg_id>", methods=["GET"])
def get_document_by_msgid(msg_id):
    if not msg_id:
        return jsonify({"ok": False, "erro": "msg_id é obrigatório"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
              raw->'document'->>'id'        AS doc_id,
              COALESCE(
                raw->'document'->>'filename',
                raw->'document'->>'file_name',
                'arquivo'
              ) AS filename,
              phone_number_id AS phone_id
            FROM mensagens
            WHERE msg_id = %s
            LIMIT 1
            """,
            (msg_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("doc_id"):
            return jsonify({"ok": False, "erro": "document id não encontrado"}), 404

        doc_id   = row["doc_id"]
        filename = row.get("filename") or "arquivo"
        phone_id = row.get("phone_id") or ""

        graph_url = f"https://graph.facebook.com/v24.0/{doc_id}"
        data, used_token = graph_get(graph_url, phone_id, timeout=10)

        lookaside_url = data.get("url")
        mime_type     = data.get("mime_type", "application/octet-stream")
        file_size     = data.get("file_size")
        if not lookaside_url:
            return jsonify({"ok": False, "erro": "URL do documento não encontrada no Graph"}), 500

        bin_resp = requests.get(
            lookaside_url,
            headers={"Authorization": f"Bearer {used_token}"},
            timeout=30
        )
        bin_resp.raise_for_status()
        content = bin_resp.content

        b64_data = base64.b64encode(content).decode("utf-8")
        data_uri = f"data:{mime_type};base64,{b64_data}"

        return jsonify({
            "ok": True,
            "data_uri": data_uri,
            "filename": filename,
            "mime_type": mime_type,
            "size_bytes": len(content) if content else file_size
        })
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --------------------------------------------------
# 📄 ROTA: download/stream do documento por msg_id
# --------------------------------------------------
@app.route("/api/conversas/document/<msg_id>/download", methods=["GET"])
def download_document_by_msgid(msg_id):
    if not msg_id:
        return jsonify({"ok": False, "erro": "msg_id é obrigatório"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
              raw->'document'->>'id'        AS doc_id,
              COALESCE(
                raw->'document'->>'filename',
                raw->'document'->>'file_name',
                'arquivo'
              ) AS filename,
              phone_number_id AS phone_id
            FROM mensagens
            WHERE msg_id = %s
            LIMIT 1
            """,
            (msg_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("doc_id"):
            return jsonify({"ok": False, "erro": "document id não encontrado"}), 404

        doc_id   = row["doc_id"]
        filename = row.get("filename") or "arquivo"
        phone_id = row.get("phone_id") or ""

        graph_url = f"https://graph.facebook.com/v24.0/{doc_id}"
        data, used_token = graph_get(graph_url, phone_id, timeout=10)

        lookaside_url = data.get("url")
        mime_type     = data.get("mime_type", "application/octet-stream")
        if not lookaside_url:
            return jsonify({"ok": False, "erro": "URL do documento não encontrada no Graph"}), 500

        bin_resp = requests.get(
            lookaside_url,
            headers={"Authorization": f"Bearer {used_token}"},
            stream=True,
            timeout=30
        )
        bin_resp.raise_for_status()

        return Response(
            bin_resp.iter_content(chunk_size=8192),
            content_type=mime_type,
            headers={
                "Content-Disposition": f'inline; filename="{filename}"',
                "Cache-Control": "no-store"
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --------------------------------------------------
# ✉️ Envia mensagem avulsa (texto ou PDF) roteando token por phone_id
# --------------------------------------------------
@app.route("/api/conversas/<telefone>", methods=["POST"])
def enviar_mensagem(telefone):
    data = request.get_json(silent=True) or {}
    texto = (data.get("texto") or "").strip()

    explicit_token = data.get("token") or None
    phone_id = data.get("phone_id") or DEFAULT_PHONE_ID
    waba_id = data.get("waba_id") or DEFAULT_WABA_ID
    msg_id = data.get("msg_id")

    pdf_url = data.get("file_url")
    filename = data.get("original_filename")

    if not phone_id:
        return jsonify({"ok": False, "erro": "phone_id não configurado"}), 400

    # payload
    if pdf_url and filename:
        payload = {
            "messaging_product": "whatsapp",
            "to": telefone,
            "type": "document",
            "document": {"link": pdf_url, "filename": filename}
        }
        conteudo = f"📎 PDF: {filename}\n🔗 {pdf_url}"
    else:
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

    try:
        r, used_token = graph_post_messages(phone_id, payload, timeout=12, explicit_token=explicit_token)
    except Exception as e:
        return jsonify({"ok": False, "erro": f"Falha na requisição para Graph API: {str(e)}"}), 500

    ok = r.ok
    status = "enviado" if ok else "erro"

    retorno_msg_id = None
    resposta_raw: Any = None
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

def _normalize_remetente(p: str) -> str:
    if not p: return ""
    d = re.sub(r"\D", "", str(p))
    if not d.startswith("55"):
        d = "55" + d
    head, rest = d[:4], d[4:]
    if len(rest) == 9 and rest.startswith("9"):
        d = head + rest[1:]
    else:
        d = head + rest
    return d

@app.route("/api/tickets/claim", methods=["POST"])
def tickets_claim():
    data = request.get_json(silent=True) or {}
    codigo   = data.get("codigo_do_agente")
    carteira = (data.get("carteira") or "").strip()

    req_remetente = _normalize_remetente(data.get("remetente") or "")
    req_phone_id  = (data.get("phone_id") or "").strip()
    prioridade    = bool(data.get("prioridade"))

    if not isinstance(codigo, int) or not carteira:
        return jsonify({"ok": False, "erro": "codigo_do_agente (int) e carteira são obrigatórios"}), 400

    phone_ids = CARTEIRA_TO_PHONE_IDS.get(carteira, [])
    if not phone_ids:
        return jsonify({"ok": False, "erro": "carteira desconhecida"}), 400
    # não force req_phone_id; quando ausente pesquisaremos em todos os phone_ids da carteira

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 1 FROM fila_de_atendimento
             WHERE codigo_do_agente=%s AND carteira=%s
        """, (codigo, carteira))
        if not cur.fetchone():
            return jsonify({"ok": False, "erro": "agente está offline nesta carteira"}), 409

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

        def _check_limit_or_409():
            try:
                cur.execute("SELECT limit_per_agent FROM tickets_limit_config WHERE carteira=%s", (carteira,))
                row_lim = cur.fetchone() or {}
                limit_per_agent = int(row_lim.get("limit_per_agent") or 0)
            except Exception:
                limit_per_agent = 0

            if limit_per_agent > 0:
                cur.execute("""
                    SELECT COUNT(*) AS qtd
                      FROM conversas_em_andamento
                     WHERE codigo_do_agente=%s
                       AND carteira=%s
                       AND ended_at IS NULL
                """, (codigo, carteira))
                qtd = int((cur.fetchone() or {}).get("qtd", 0))
                if qtd >= limit_per_agent:
                    return jsonify({
                        "ok": False,
                        "erro": f"Limite de {limit_per_agent} tickets simultâneos atingido para esta carteira, fale com seu supervisor",
                        "limit_per_agent": limit_per_agent,
                        "ativos": qtd,
                        "carteira": carteira
                    }), 409
            return None

        # ===== FLUXO DIRECIONADO =====
        if req_remetente:
            cur.execute(r"""
                SELECT codigo_do_agente, nome_agente
                  FROM conversas_em_andamento
                 WHERE (
                        regexp_replace(telefone, '(?<=^55\\d{2})9','') = regexp_replace(%s,'(?<=^55\\d{2})9','')
                        OR regexp_replace(telefone, '(?<=^55\\d{2})9','') = %s
                       )
                   AND phone_id=%s
                   AND ended_at IS NULL
                 LIMIT 1
            """, (req_remetente, req_remetente, req_phone_id))
            row = cur.fetchone()
            if row:
                if int(row["codigo_do_agente"]) == int(codigo):
                    cur.execute("""
                        WITH msgs AS (
                            SELECT data_hora,
                                   CASE WHEN direcao='in'
                                        THEN data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                                        ELSE data_hora END AS dh_adj,
                                   mensagem AS mensagem_final
                              FROM mensagens
                             WHERE (remetente = %s OR telefone_norm = %s)
                               AND phone_number_id= %s
                             ORDER BY data_hora DESC
                             LIMIT 1
                        )
                        SELECT mensagem_final, dh_adj AS data_hora FROM msgs
                    """, (req_remetente, req_remetente, req_phone_id))
                    last = cur.fetchone() or {}
                    return jsonify({
                        "ok": True,
                        "ticket": {
                            "remetente": req_remetente,
                            "phone_id": req_phone_id,
                            "nome_exibicao": req_remetente,
                            "mensagem_final": last.get("mensagem_final"),
                            "data_hora": last.get("data_hora").isoformat() if last.get("data_hora") else None
                        }
                    })
                return jsonify({
                    "ok": False,
                    "erro": "Conversa já está em atendimento por outro agente",
                    "assigned_to": {
                        "codigo": row["codigo_do_agente"],
                        "nome": row["nome_agente"]
                    }
                }), 409

            limited = _check_limit_or_409()
            if limited:
                return limited

            sql_check = r"""
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
                SELECT data_hora, remetente AS telefone, telefone_norm,
                       phone_number_id AS phone_id,
                       direcao AS status, mensagem AS mensagem_final, msg_id
                FROM mensagens
            ),
            conversas AS (
                SELECT data_hora, telefone,
                       regexp_replace(telefone, '(?<=^55\\d{2})9', '', 'g') AS telefone_norm,
                       phone_id, status, mensagem_final, ''::text AS msg_id
                  FROM enviados
                UNION
                SELECT data_hora, telefone, telefone_norm, phone_id, status, mensagem_final, msg_id
                  FROM cliente_msg
                UNION
                SELECT data_hora, remetente AS telefone, telefone_norm, phone_id,
                       status, conteudo AS mensagem_final, ''::text AS msg_id
                  FROM mensagens_avulsas
                 WHERE status <> 'erro'
            ),
            msg_id AS (
                SELECT remetente, telefone_norm, msg_id FROM (
                    SELECT data_hora, remetente, telefone_norm, msg_id,
                           row_number() OVER (PARTITION BY remetente ORDER BY data_hora DESC) AS idx
                      FROM mensagens
                ) t WHERE idx = 1
            ),
            ranked AS (
                SELECT a.telefone, b.telefone_norm, a.phone_id, a.status, a.mensagem_final, a.data_hora,
                       b.msg_id,
                       row_number() OVER (
                          PARTITION BY a.telefone, a.phone_id
                          ORDER BY CASE WHEN a.status='in'
                                        THEN a.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                                        ELSE a.data_hora END DESC
                       ) AS rn
                  FROM conversas a
                  JOIN msg_id b
                    ON a.telefone = b.remetente
                    OR a.telefone = b.telefone_norm
            ),
            last_in AS (
                SELECT telefone_norm AS telefone,
                       phone_number_id AS phone_id,
                       MAX(CASE WHEN direcao<>'in'
                                THEN data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                                ELSE data_hora END) AS last_in
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
               AND r.phone_id = ANY(%s::text[])
               AND (r.telefone = %s OR r.telefone_norm = %s)
               AND (CASE WHEN r.status='in'
                         THEN r.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                         ELSE r.data_hora END) >= now() - interval '1 day'
               AND (tb.telefone IS NULL
                    OR (li.last_in IS NOT NULL
                        AND li.last_in > (tb.bloqueado_at AT TIME ZONE 'America/Sao_Paulo') AT TIME ZONE 'UTC'))
             LIMIT 1
            """
            ids_for_search = [req_phone_id] if req_phone_id else phone_ids
            cur.execute(sql_check, (ids_for_search, req_remetente, req_remetente))
            cand = cur.fetchone()
            if not cand:
                return jsonify({"ok": False, "erro": "Contato não está na fila desta carteira"}), 404

            try:
                cur.execute("""
                    INSERT INTO conversas_em_andamento
                    (telefone, phone_id, carteira, codigo_do_agente, nome_agente)
                    VALUES
                    (%s, %s, %s, %s, (SELECT nome FROM agentes WHERE codigo_do_agente=%s))
                    ON CONFLICT (telefone, phone_id) WHERE ended_at IS NULL DO NOTHING
                    RETURNING telefone
                """, (req_remetente, cand["phone_id"], carteira, codigo, codigo))
                got = cur.fetchone()
                if got:
                    conn.commit()
                    return jsonify({
                        "ok": True,
                        "ticket": {
                            "remetente": cand["remetente"],
                            "phone_id": cand["phone_id"],
                            "nome_exibicao": cand["nome_exibicao"] or cand["remetente"],
                            "mensagem_final": cand["mensagem_final"],
                            "data_hora": cand["data_hora"].isoformat() if cand["data_hora"] else None
                        }
                    })
                conn.rollback()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                cur.execute("""
                    SELECT codigo_do_agente, nome_agente
                      FROM conversas_em_andamento
                     WHERE (
                            regexp_replace(telefone, '(?<=^55\\d{2})9','') = regexp_replace(%s,'(?<=^55\\d{2})9','')
                            OR regexp_replace(telefone, '(?<=^55\\d{2})9','') = %s
                           )
                       AND phone_id=%s
                       AND ended_at IS NULL
                     LIMIT 1
                """, (req_remetente, req_remetente, req_phone_id))
                holder = cur.fetchone()
                if holder:
                    return jsonify({
                        "ok": False,
                        "erro": "Conversa já está em atendimento por outro agente",
                        "assigned_to": {
                            "codigo": holder["codigo_do_agente"],
                            "nome": holder["nome_agente"]
                        }
                    }), 409
                return jsonify({"ok": False, "erro": "Não foi possível reservar, tente remover o dígito 9"}), 409

            return jsonify({"ok": False, "erro": "Não foi possível reservar"}), 409

        # ===== FLUXO NORMAL =====
        limited = _check_limit_or_409()
        if limited:
            return limited

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
                SELECT data_hora, remetente AS telefone, telefone_norm,
                       phone_number_id AS phone_id,
                       direcao AS status, mensagem AS mensagem_final, msg_id
                FROM mensagens
            ),
            conversas AS (
                SELECT data_hora, telefone,
                       regexp_replace(telefone, '(?<=^55\\d{2})9', '', 'g') AS telefone_norm,
                       phone_id, status, mensagem_final, ''::text AS msg_id
                  FROM enviados
                UNION
                SELECT data_hora, telefone, telefone_norm, phone_id, status, mensagem_final, msg_id
                  FROM cliente_msg
                UNION
                SELECT data_hora, remetente AS telefone, telefone_norm, phone_id,
                       status, conteudo AS mensagem_final, ''::text AS msg_id
                  FROM mensagens_avulsas
                 WHERE status <> 'erro'
            ),
            msg_id AS (
                SELECT remetente, telefone_norm, msg_id FROM (
                    SELECT data_hora, remetente, telefone_norm, msg_id,
                           row_number() OVER (PARTITION BY remetente ORDER BY data_hora DESC) AS idx
                      FROM mensagens
                ) t WHERE idx = 1
            ),
            ranked AS (
                SELECT a.telefone, a.phone_id, a.status, a.mensagem_final, a.data_hora,
                       b.msg_id,
                       row_number() OVER (
                          PARTITION BY a.telefone, a.phone_id
                          ORDER BY CASE WHEN a.status='in'
                                        THEN a.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                                        ELSE a.data_hora END DESC
                       ) AS rn
                  FROM conversas a
                  JOIN msg_id b
                    ON a.telefone = b.remetente
                    OR a.telefone = b.telefone_norm
            ),
            last_in AS (
                SELECT remetente AS telefone,
                       phone_number_id AS phone_id,
                       MAX(CASE WHEN direcao<>'in'
                                THEN data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                                ELSE data_hora END) AS last_in
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
               AND r.phone_id = ANY(%s::text[])
               AND (CASE WHEN r.status='in'
                         THEN r.data_hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                         ELSE r.data_hora END) >= now() - interval '1 day'
               AND (tb.telefone IS NULL
                    OR (li.last_in IS NOT NULL
                        AND li.last_in > (tb.bloqueado_at AT TIME ZONE 'America/Sao_Paulo') AT TIME ZONE 'UTC'))
             ORDER BY (r.status='in') DESC, data_hora DESC
        """
        cur.execute(sql, (phone_ids,))
        candidatos = cur.fetchall()

        for c in candidatos:
            try:
                cur.execute("""
                    INSERT INTO conversas_em_andamento
                    (telefone, phone_id, carteira, codigo_do_agente, nome_agente)
                    VALUES
                    (%s, %s, %s, %s, (SELECT nome FROM agentes WHERE codigo_do_agente=%s))
                    ON CONFLICT (telefone, phone_id) WHERE ended_at IS NULL DO NOTHING
                    RETURNING telefone
                """, (c["remetente"], c["phone_id"], carteira, codigo, codigo))
                row = cur.fetchone()
                if row:
                    conn.commit()
                    return jsonify({
                        "ok": True,
                        "ticket": {
                            "remetente": c["remetente"],
                            "phone_id": phone_ids,
                            "nome_exibicao": c["nome_exibicao"] or c["remetente"],
                            "mensagem_final": c["mensagem_final"],
                            "data_hora": c["data_hora"].isoformat() if c["data_hora"] else None
                        }
                    })
                conn.rollback()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                cur.execute("""
                    SELECT codigo_do_agente, nome_agente
                      FROM conversas_em_andamento
                     WHERE (
                            regexp_replace(telefone, '(?<=^55\\d{2})9','') = regexp_replace(%s,'(?<=^55\\d{2})9','')
                            OR regexp_replace(telefone, '(?<=^55\\d{2})9','') = %s
                           )
                       AND phone_id=%s
                       AND ended_at IS NULL
                     LIMIT 1
                """, (c["remetente"], c["remetente"], c["phone_id"]))
                holder = cur.fetchone()
                if holder:
                    return jsonify({
                        "ok": False,
                        "erro": "Conversa já está em atendimento por outro agente",
                        "assigned_to": {
                            "codigo": holder["codigo_do_agente"],
                            "nome": holder["nome_agente"]
                        }
                    }), 409
                continue

        return jsonify({"ok": False, "erro": "Sem conversas disponíveis nesta carteira"}), 404

    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"claim falhou: {str(e)}"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/tickets/contagem_por_agente", methods=["GET"])
def tickets_contagem_por_agente():
    carteira = (request.args.get("carteira") or "").strip()
    conn = get_conn(); cur = conn.cursor()
    try:
        # conta tickets ativos direto na tabela leve
        sql = """
          SELECT
            codigo_do_agente,
            carteira,
            COUNT(*)::int AS qtd
          FROM conversas_em_andamento
          WHERE ended_at IS NULL
          {}
          GROUP BY 1,2
        """.format("AND carteira=%s" if carteira else "")
        cur.execute(sql, (carteira,) if carteira else ())
        rows = cur.fetchall()
        mapa = {f"{r['codigo_do_agente']}-{r['carteira']}": r['qtd'] for r in rows}
        return jsonify({"ok": True, "mapa": mapa})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500
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
                SELECT data_hora, remetente AS telefone,telefone_norm, phone_number_id AS phone_id,
                       direcao AS status, mensagem AS mensagem_final, msg_id
                FROM mensagens
            ),
            conversas AS (
                SELECT data_hora,telefone,
                       regexp_replace(telefone, '(?<=^55\\d{2})9', '', 'g') AS telefone_norm,
                       phone_id, status, mensagem_final, ''::text AS msg_id
                FROM enviados
                UNION
                SELECT data_hora, telefone,telefone_norm, phone_id, status, mensagem_final, msg_id
                FROM cliente_msg
                UNION
                SELECT data_hora, remetente AS telefone,telefone_norm, phone_id, status, conteudo AS mensagem_final, ''::text AS msg_id
                FROM mensagens_avulsas WHERE status <> 'erro'
            ),
            msg_id AS (
                SELECT remetente,telefone_norm, msg_id FROM (
                    SELECT data_hora, remetente,telefone_norm, msg_id,
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
                        OR a.telefone = b.telefone_norm
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

#Nova função
@app.route("/api/supervisao/tickets-limit", methods=["GET"])
def get_tickets_limit():
    carteira = request.args.get("carteira")
    conn = get_conn(); cur = conn.cursor()
    try:
        if carteira:
            cur.execute("""
                SELECT limit_per_agent
                  FROM tickets_limit_config
                 WHERE carteira = %s
            """, (carteira,))
            row = cur.fetchone()
            return jsonify({
                "carteira": carteira,
                "limit_per_agent": int((row or {}).get("limit_per_agent") or 0)
            })
        else:
            cur.execute("""
                SELECT carteira, limit_per_agent, updated_at
                  FROM tickets_limit_config
                 ORDER BY carteira
            """)
            rows = cur.fetchall() or []
            return jsonify([
                {
                    "carteira": r["carteira"],
                    "limit_per_agent": int(r["limit_per_agent"] or 0),
                    "updated_at": r["updated_at"].isoformat()
                } for r in rows
            ])
    finally:
        cur.close(); conn.close()

#nova função
@app.route("/api/supervisao/tickets-limit", methods=["PUT"])
def put_tickets_limit():
    data = request.get_json(silent=True) or {}
    carteira = (data.get("carteira") or "").strip()
    if not carteira:
        return jsonify({"ok": False, "erro": "Informe a carteira"}), 400

    try:
        lim = int(data.get("limit_per_agent", 0))
    except Exception:
        return jsonify({"ok": False, "erro": "limit_per_agent inválido"}), 400

    if lim < 0 or lim > 50:
        return jsonify({"ok": False, "erro": "Informe um valor entre 0 e 50"}), 400

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO tickets_limit_config (carteira, limit_per_agent, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (carteira) DO UPDATE
               SET limit_per_agent = EXCLUDED.limit_per_agent,
                   updated_at = NOW()
        """, (carteira, lim))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Falha ao salvar: {str(e)}"}), 500
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

# 🔎 Monitoria: listar conversas_em_andamento com filtros
@app.route("/api/monitoria/em_andamento", methods=["GET"])
def monitoria_em_andamento():
    carteira     = (request.args.get("carteira") or "").strip()
    nome_agente  = (request.args.get("nome_agente") or "").strip()
    telefone     = (request.args.get("telefone") or "").strip()
    status       = (request.args.get("status") or "todas").strip().lower()
    try:
        limit = int(request.args.get("limit", "100"))
        if limit <= 0 or limit > 1000:
            limit = 100
    except Exception:
        limit = 100

    conn = get_conn()
    cur  = conn.cursor()
    try:
        sql = """
            SELECT
              telefone,
              phone_id,
              carteira,
              codigo_do_agente,
              nome_agente,
              started_at,
              ended_at
            FROM conversas_em_andamento
            WHERE 1=1
        """
        params = []

        if carteira:
            sql += " AND carteira = %s"
            params.append(carteira)

        if nome_agente:
            sql += " AND nome_agente ILIKE %s"
            params.append(f"%{nome_agente}%")

        if telefone:
            sql += " AND telefone ILIKE %s"
            params.append(f"%{telefone}%")

        if status == "ativas":
            sql += " AND ended_at IS NULL"
        elif status == "finalizadas":
            sql += " AND ended_at IS NOT NULL"

        sql += """
            ORDER BY (ended_at IS NULL) DESC, started_at DESC
            LIMIT %s
        """
        params.append(limit)

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"ok": False, "erro": f"monitoria/em_andamento falhou: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 6000))
    app.run(host="0.0.0.0", port=port, debug=True)

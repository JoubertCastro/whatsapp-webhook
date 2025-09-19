from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime, timezone, timedelta
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2, psycopg2.extras
import json, os
from zoneinfo import ZoneInfo

# =========================
# Config
# =========================
app = Flask(__name__)
CORS(app)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/postgres"
)
VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "meu_token_secreto")
TZ = ZoneInfo("America/Sao_Paulo")

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # --- WhatsApp
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mensagens (
            id SERIAL PRIMARY KEY,
            data_hora TIMESTAMP,
            remetente TEXT,
            mensagem TEXT,
            direcao TEXT,
            nome TEXT,
            msg_id TEXT,
            phone_number_id TEXT,
            display_phone_number TEXT,
            raw JSONB
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS ix_mensagens_msg_id ON mensagens(msg_id);")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS status_mensagens (
            id SERIAL PRIMARY KEY,
            data_hora TIMESTAMP,
            msg_id TEXT,
            recipient_id TEXT,
            status TEXT,
            phone_number_id TEXT,
            display_phone_number TEXT,
            raw JSONB
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS ix_status_msg_fast
        ON status_mensagens (recipient_id, msg_id, display_phone_number, data_hora DESC);
    """)

    # --- Usuários / Login
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            senha TEXT NOT NULL,
            ativo BOOLEAN NOT NULL DEFAULT TRUE,
            criado_em TIMESTAMP DEFAULT NOW()
        );
    """)

    # --- Envios
    cur.execute("""
        CREATE TABLE IF NOT EXISTS envios (
            id SERIAL PRIMARY KEY,
            nome_disparo TEXT NOT NULL,
            grupo_trabalho TEXT,
            criado_em TIMESTAMP DEFAULT NOW(),
            modo_envio TEXT,
            data_hora_agendamento TIMESTAMP,
            intervalo_msg INT,
            tamanho_lote INT,
            intervalo_lote INT,
            template JSONB,
            token TEXT,
            phone_id TEXT,
            waba_id TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS envios_analitico (
            id SERIAL PRIMARY KEY,
            envio_id INT REFERENCES envios(id) ON DELETE CASCADE,
            nome_disparo TEXT,
            grupo_trabalho TEXT,
            data_hora TIMESTAMP DEFAULT NOW(),
            telefone TEXT,
            conteudo TEXT,
            status TEXT DEFAULT 'pendente'
        );
    """)

    # --- Agentes e Fila
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agentes (
            codigo_do_agente INT PRIMARY KEY,
            nome TEXT NOT NULL,
            carteira TEXT NOT NULL,
            origem_bd TEXT NOT NULL,
            criado_em TIMESTAMP DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fila_de_atendimento (
            id SERIAL PRIMARY KEY,
            codigo_do_agente INT NOT NULL REFERENCES agentes(codigo_do_agente) ON DELETE CASCADE,
            carteira TEXT NOT NULL,
            data_hora TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(codigo_do_agente, carteira)
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS ix_fila_carteira_ord
        ON fila_de_atendimento (carteira, data_hora ASC);
    """)

    conn.commit()
    cur.close()
    conn.close()

init_db()

# =========================
# Utils
# =========================
def bad_request(msg):  return jsonify({"ok": False, "erro": msg}), 400
def conflict(msg):     return jsonify({"ok": False, "erro": msg}), 409
def not_found(msg):    return jsonify({"ok": False, "erro": msg}), 404

def ajustar_timestamp(ts: str):
    """
    ts pode ser epoch em segundos (string). Se vier inválido, usa 'agora' em TZ São Paulo.
    """
    try:
        # WhatsApp geralmente manda epoch em segundos
        dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return dt_utc.astimezone(TZ)
    except Exception:
        return datetime.now(TZ)

def salvar_mensagem(remetente, mensagem, msg_id=None, nome=None, timestamp=None,
                    raw=None, phone_number_id=None, display_phone_number=None):
    data_hora = ajustar_timestamp(timestamp) if timestamp else datetime.now(TZ)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO mensagens (data_hora, remetente, mensagem, direcao, nome, msg_id, phone_number_id, display_phone_number, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (data_hora, remetente, mensagem, "in", nome, msg_id, phone_number_id, display_phone_number, json.dumps(raw) if raw else None)
    )
    conn.commit()
    cur.close()
    conn.close()

def salvar_status(msg_id, recipient_id, status, raw, timestamp=None,
                  phone_number_id=None, display_phone_number=None):
    data_hora = ajustar_timestamp(timestamp) if timestamp else datetime.now(TZ)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO status_mensagens (data_hora, msg_id, recipient_id, status, phone_number_id, display_phone_number, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (data_hora, msg_id, recipient_id, status, phone_number_id, display_phone_number, json.dumps(raw))
    )
    conn.commit()
    cur.close()
    conn.close()

# =========================
# Webhook Meta
# =========================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        if mode == "subscribe" and token == VERIFY_TOKEN:
            return challenge, 200
        return "Erro de validação", 403

    data = request.get_json(silent=True) or {}
    print("📩 Recebi:", json.dumps(data, indent=2, ensure_ascii=False))

    try:
        entry = (data.get("entry") or [{}])[0]
        changes = (entry.get("changes") or [{}])[0]
        value = changes.get("value", {})

        phone_number_id = value.get("metadata", {}).get("phone_number_id")
        display_phone_number = value.get("metadata", {}).get("display_phone_number")

        # Mensagens
        for msg in value.get("messages", []):
            remetente = msg.get("from", "desconhecido")
            msg_id = msg.get("id")
            tipo = msg.get("type")
            if tipo == "text":
                texto = msg.get("text", {}).get("body")
            elif tipo == "button":
                b = msg.get("button", {})
                texto = f"{b.get('text')} (payload: {b.get('payload')})"
            else:
                texto = f"[{tipo}]"

            nome = (value.get("contacts") or [{}])[0].get("profile", {}).get("name")

            salvar_mensagem(
                remetente, texto,
                msg_id=msg_id, nome=nome, timestamp=msg.get("timestamp"),
                raw=msg, phone_number_id=phone_number_id, display_phone_number=display_phone_number
            )

        # Status
        for st in value.get("statuses", []):
            salvar_status(
                st.get("id"), st.get("recipient_id"), st.get("status"), st,
                st.get("timestamp"), phone_number_id=phone_number_id, display_phone_number=display_phone_number
            )

    except Exception as e:
        print("❌ Erro ao processar webhook:", e)

    return "EVENT_RECEIVED", 200

# =========================
# Saúde
# =========================
@app.route("/api/saude")
def saude():
    return jsonify({"ok": True})

# =========================
# Usuários (Auth simples)
# =========================
@app.route("/api/cadastrar", methods=["POST"])
def cadastrar():
    data = request.get_json(silent=True) or {}
    nome = (data.get("nome") or "").strip()
    email = (data.get("email") or "").strip().lower()
    senha = data.get("senha") or ""

    if not nome or not email or not senha:
        return bad_request("nome, email e senha são obrigatórios")

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM usuarios WHERE email=%s", (email,))
        if cur.fetchone():
            return conflict("email já cadastrado")

        cur.execute("""
            INSERT INTO usuarios (nome, email, senha, criado_em)
            VALUES (%s,%s,%s,NOW())
            RETURNING id, criado_em
        """, (nome, email, generate_password_hash(senha)))
        row = cur.fetchone()
        conn.commit()
        return jsonify({"ok": True, "id": row["id"], "criado_em": row["criado_em"]})
    except Exception as e:
        conn.rollback()
        print("❌ /api/cadastrar:", e)
        return jsonify({"ok": False, "erro": "erro ao cadastrar usuário"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    senha = data.get("senha") or ""
    if not email or not senha:
        return bad_request("email e senha são obrigatórios")

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT id, nome, email, senha, ativo FROM usuarios WHERE email=%s", (email,))
        user = cur.fetchone()
    finally:
        cur.close(); conn.close()

    if not user or not check_password_hash(user["senha"], senha):
        return jsonify({"ok": False, "erro": "credenciais inválidas"}), 401
    if not user["ativo"]:
        return jsonify({"ok": False, "erro": "usuário inativo"}), 403

    return jsonify({"ok": True, "user": {"id": user["id"], "nome": user["nome"], "email": user["email"], "ativo": user["ativo"]}})

@app.route("/api/usuarios", methods=["GET"])
def listar_usuarios():
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT id, nome, email, ativo, criado_em FROM usuarios WHERE ativo=true ORDER BY id")
        return jsonify(cur.fetchall())
    except Exception as e:
        print("❌ /api/usuarios [GET]:", e)
        return jsonify({"ok": False, "erro": "erro ao listar usuários"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/usuarios/<int:id>", methods=["PUT"])
def editar_usuario(id):
    data = request.get_json(silent=True) or {}
    nome = (data.get("nome") or "").strip()
    email = (data.get("email") or "").strip().lower()
    senha = data.get("senha") or ""
    if not nome or not email or not senha:
        return bad_request("nome, email e senha são obrigatórios")

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("UPDATE usuarios SET nome=%s, email=%s, senha=%s WHERE id=%s AND ativo=true",
                    (nome, email, generate_password_hash(senha), id))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        print("❌ /api/usuarios/<id> [PUT]:", e)
        return jsonify({"ok": False, "erro": "erro ao editar usuário"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/usuarios/<int:id>", methods=["DELETE"])
def excluir_usuario(id):
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("UPDATE usuarios SET ativo=false WHERE id=%s", (id,))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        print("❌ /api/usuarios/<id> [DELETE]:", e)
        return jsonify({"ok": False, "erro": "erro ao excluir usuário"}), 500
    finally:
        cur.close(); conn.close()

# =========================
# Status (último por mensagem)
# =========================
@app.route("/api/status", methods=["GET"])
def listar_status():
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT data_hora, recipient_id, status, display_phone_number
            FROM (
                SELECT data_hora, recipient_id, status, display_phone_number,
                       row_number() OVER (PARTITION BY recipient_id, msg_id, display_phone_number ORDER BY data_hora DESC) AS atual
                FROM status_mensagens
            ) tb
            WHERE atual=1
            ORDER BY data_hora DESC
        """)
        return jsonify(cur.fetchall())
    except Exception as e:
        print("❌ /api/status:", e)
        return jsonify({"ok": False, "erro": "erro ao buscar status"}), 500
    finally:
        cur.close(); conn.close()

# =========================
# Agentes
# =========================
@app.route("/api/agentes/<int:codigo>", methods=["GET"])
def get_agente(codigo: int):
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT codigo_do_agente, nome, carteira, origem_bd
            FROM agentes WHERE codigo_do_agente=%s
        """, (codigo,))
        row = cur.fetchone()
        if not row:
            return not_found("Esse agente ainda não está cadastrado")
        return jsonify({"ok": True, "agente": row})
    except Exception as e:
        print("❌ /api/agentes/<codigo> GET:", e)
        return jsonify({"ok": False, "erro": "Erro ao buscar agente"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/agentes", methods=["POST"])
def post_agente():
    data = request.get_json(silent=True) or {}
    codigo   = data.get("codigo_do_agente")
    nome     = (data.get("nome") or "").strip()
    carteira = data.get("carteira")
    origem   = data.get("origem_bd")

    if not isinstance(codigo, int) or codigo <= 0:
        return bad_request("codigo_do_agente deve ser inteiro positivo")
    if not nome or len(nome.split()) < 2:
        return bad_request("Informe o nome completo")
    if not carteira:
        return bad_request("Selecione a carteira")
    if not origem:
        return bad_request("Selecione a origem_bd")

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM agentes WHERE codigo_do_agente=%s", (codigo,))
        if cur.fetchone():
            return conflict("Agente já cadastrado")

        cur.execute("""
            INSERT INTO agentes (codigo_do_agente, nome, carteira, origem_bd)
            VALUES (%s,%s,%s,%s)
            RETURNING codigo_do_agente
        """, (codigo, nome, carteira, origem))
        row = cur.fetchone()
        conn.commit()
        return jsonify({"ok": True, "codigo_do_agente": row["codigo_do_agente"]})
    except Exception as e:
        conn.rollback()
        print("❌ /api/agentes POST:", e)
        return jsonify({"ok": False, "erro": "Erro ao cadastrar agente"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/agentes/<int:codigo>", methods=["PUT"])
def put_agente(codigo: int):
    data = request.get_json(silent=True) or {}
    nome     = (data.get("nome") or "").strip() if data.get("nome") else None
    carteira = data.get("carteira")

    if not nome and not carteira:
        return bad_request("Informe nome e/ou carteira para atualizar")

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM agentes WHERE codigo_do_agente=%s", (codigo,))
        if not cur.fetchone():
            return not_found("Esse agente não está cadastrado")

        sets, params = [], []
        if nome:
            sets.append("nome=%s"); params.append(nome)
        if carteira:
            sets.append("carteira=%s"); params.append(carteira)
        params.append(codigo)

        cur.execute(f"UPDATE agentes SET {', '.join(sets)} WHERE codigo_do_agente=%s", params)
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        print("❌ /api/agentes/<codigo> PUT:", e)
        return jsonify({"ok": False, "erro": "Erro ao editar agente"}), 500
    finally:
        cur.close(); conn.close()

# =========================
# Fila de Atendimento
# =========================
@app.route("/api/fila/online", methods=["POST"])
def fila_online():
    data = request.get_json(silent=True) or {}
    codigo   = data.get("codigo_do_agente")
    carteira = data.get("carteira")

    if not isinstance(codigo, int) or codigo <= 0:
        return bad_request("codigo_do_agente deve ser inteiro positivo")
    if not carteira:
        return bad_request("carteira é obrigatória")

    conn = get_conn(); cur = conn.cursor()
    try:
        # Garante que o agente existe
        cur.execute("SELECT 1 FROM agentes WHERE codigo_do_agente=%s", (codigo,))
        if not cur.fetchone():
            return not_found("Agente não cadastrado")

        # Tenta entrar na fila (sem estourar exceção)
        cur.execute("""
            INSERT INTO fila_de_atendimento (codigo_do_agente, carteira, data_hora)
            VALUES (%s, %s, NOW())
            ON CONFLICT (codigo_do_agente, carteira) DO NOTHING
            RETURNING id, data_hora
        """, (codigo, carteira))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return conflict("Já existe um agente com esse código online nessa carteira")
        conn.commit()
        return jsonify({"ok": True, "id": row["id"], "data_hora": row["data_hora"].isoformat()})
    except Exception as e:
        conn.rollback()
        print("❌ /api/fila/online:", e)
        return jsonify({"ok": False, "erro": "Erro ao entrar online"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/fila/offline", methods=["DELETE"])
def fila_offline():
    data = request.get_json(silent=True) or {}
    codigo   = data.get("codigo_do_agente")
    carteira = data.get("carteira")

    if not isinstance(codigo, int) or codigo <= 0:
        return bad_request("codigo_do_agente deve ser inteiro positivo")
    if not carteira:
        return bad_request("carteira é obrigatória")

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM fila_de_atendimento
            WHERE codigo_do_agente=%s AND carteira=%s
            RETURNING id
        """, (codigo, carteira))
        row = cur.fetchone()
        conn.commit()
        if not row:
            return not_found("Esse agente não está online nessa carteira")
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        print("❌ /api/fila/offline:", e)
        return jsonify({"ok": False, "erro": "Erro ao ficar offline"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/fila/status", methods=["GET"])
def fila_status():
    carteira = request.args.get("carteira")
    conn = get_conn(); cur = conn.cursor()
    try:
        if carteira:
            cur.execute("""
                SELECT f.codigo_do_agente, a.nome, f.carteira, f.data_hora
                FROM fila_de_atendimento f
                JOIN agentes a ON a.codigo_do_agente=f.codigo_do_agente
                WHERE f.carteira=%s
                ORDER BY f.data_hora ASC
            """, (carteira,))
        else:
            cur.execute("""
                SELECT f.codigo_do_agente, a.nome, f.carteira, f.data_hora
                FROM fila_de_atendimento f
                JOIN agentes a ON a.codigo_do_agente=f.codigo_do_agente
                ORDER BY f.carteira, f.data_hora ASC
            """)
        return jsonify(cur.fetchall())
    except Exception as e:
        print("❌ /api/fila/status:", e)
        return jsonify({"ok": False, "erro": "Erro ao listar fila"}), 500
    finally:
        cur.close(); conn.close()

# =========================
# Run
# =========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)

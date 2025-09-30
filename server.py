from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime, timezone, timedelta
from werkzeug.security import generate_password_hash, check_password_hash
from virtual_agent import handle_incoming, ALLOWED_PHONE_IDS, ALLOWED_WABA_IDS
import psycopg2
import psycopg2.extras
import json
import os

def _bot_account_allowed(phone_id: str, waba_id: str|None) -> tuple[bool, str]:
    """
    Retorna (allowed, flow_file). Verifica DB (bot_accounts) e fallback p/ ENV.
    """
    allowed = False
    flow_file = "flows/onboarding.yaml"
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT enabled, flow_file FROM bot_accounts WHERE phone_id=%s", (phone_id,))
        row = cur.fetchone()
        if row:
            allowed = bool(row["enabled"])
            flow_file = row["flow_file"] or flow_file
        cur.close(); conn.close()
    except Exception:
        pass

    # fallback ENV
    if not allowed and ALLOWED_PHONE_IDS:
        if str(phone_id) in ALLOWED_PHONE_IDS:
            allowed = True

    # checagem (opcional) por WABA
    if allowed and ALLOWED_WABA_IDS and waba_id:
        if str(waba_id) not in ALLOWED_WABA_IDS:
            allowed = False

    return allowed, flow_file

# =========================
# Config
# =========================
app = Flask(__name__)
CORS(app)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"
)
VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "meu_token_secreto")

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
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc) - timedelta(hours=3)
    except Exception:
        return datetime.now(timezone.utc) - timedelta(hours=3)


def salvar_mensagem(remetente, mensagem, msg_id=None, nome=None, timestamp=None,
                    raw=None, phone_number_id=None, display_phone_number=None):
    data_hora = ajustar_timestamp(timestamp) if timestamp else datetime.now(timezone.utc) - timedelta(hours=3)
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
    data_hora = ajustar_timestamp(timestamp) if timestamp else datetime.now(timezone.utc) - timedelta(hours=3)
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

        messages = value.get("messages", [])
        statuses = value.get("statuses", [])

        # 1) Salva mensagens recebidas
        for msg in messages:
            remetente = msg.get("from", "desconhecido")
            msg_id = msg.get("id")
            tipo = msg.get("type")

            if tipo == "text":
                texto = msg.get("text", {}).get("body")
            elif tipo == "interactive":
                inter = msg.get("interactive", {})
                btn = inter.get("button_reply") or {}
                texto = btn.get("title") or btn.get("id") or "[interactive]"
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

        # 2) Gate por conta + agente virtual
        waba_id = value.get("business", {}).get("id") or value.get("waba_id")
        allowed, flow_file = _bot_account_allowed(phone_number_id, waba_id)

        if allowed:
            for msg in messages:
                remetente = msg.get("from", "desconhecido")
                tipo = msg.get("type")

                # Texto para o agente
                texto = None
                if tipo == "text":
                    texto = msg.get("text", {}).get("body")
                elif tipo == "interactive":
                    inter = msg.get("interactive", {})
                    btn = inter.get("button_reply") or {}
                    texto = btn.get("id") or btn.get("title")
                elif tipo == "button":
                    b = msg.get("button", {})
                    texto = b.get("payload") or b.get("text")

                contact = {
                    "nome": (value.get("contacts") or [{}])[0].get("profile", {}).get("name"),
                    "cpf": None
                }

                # Chama o agente para CADA mensagem
                handle_incoming(remetente, texto, flow_file=flow_file, contact=contact)

        # 3) Status
        for st in statuses:
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

@app.route("/api/envios", methods=["POST"])
def criar_envio():
    data = request.get_json(silent=True) or {}
    nome = data.get("nome_disparo")
    grupo = data.get("grupo_trabalho")
    modo = data.get("modo_envio")
    agendamento = data.get("data_hora_agendamento")
    intervalo_msg = data.get("intervalo_msg")
    tamanho_lote = data.get("tamanho_lote")
    intervalo_lote = data.get("intervalo_lote")
    contatos = data.get("contatos", [])

    if not nome or not grupo:
        return jsonify({"ok": False, "erro": "nome_disparo e grupo_trabalho são obrigatórios"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO envios (nome_disparo, grupo_trabalho, modo_envio, data_hora_agendamento,
                                intervalo_msg, tamanho_lote, intervalo_lote,
                                template, token, phone_id, waba_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (
            nome, grupo, modo, agendamento,
            intervalo_msg, tamanho_lote, intervalo_lote,
            json.dumps(data.get("template")),
            data.get("token"),    
            data.get("phone_id"), 
            data.get("waba_id")   
        ))

        envio_id = cur.fetchone()["id"]

        for c in contatos:
            cur.execute("""
                INSERT INTO envios_analitico (envio_id, nome_disparo, grupo_trabalho, telefone, conteudo, status)
                VALUES (%s,%s,%s,%s,%s,'pendente')
            """, (envio_id, nome, grupo, c.get("telefone"), c.get("conteudo")))


        conn.commit()
        return jsonify({"ok": True, "id": envio_id})
    except Exception as e:
        conn.rollback()
        print("❌ Erro ao salvar envio:", e)
        return jsonify({"ok": False, "erro": "erro ao salvar envio"}), 500
    finally:
        cur.close()
        conn.close()

# =========================
# Agendamentos (CRUD + ações)
# =========================

def _row_to_iso(dt):
    # Converte datetime -> string ISO (mantém 'None' como None)
    return dt.isoformat() if dt else None

@app.route("/api/envios", methods=["GET"])
def listar_envios():
    """
    Lista envios (imediatos e agendados) com agregados de status do analítico.
    Aceita filtros opcionais via querystring:
      - modo_envio=imediato|agendar
      - status=pendente|pausado|cancelado|enviado|erro (filtra pelo analítico predominante)
    """
    modo = request.args.get("modo_envio")
    status_filtro = request.args.get("status")

    conn = get_conn(); cur = conn.cursor()
    try:
        # Agregados por envio
        cur.execute("""
            SELECT
              e.id,
              e.nome_disparo,
              e.grupo_trabalho,
              e.criado_em,
              e.modo_envio,
              e.data_hora_agendamento,
              e.intervalo_msg,
              e.tamanho_lote,
              e.intervalo_lote,
              COALESCE(SUM( (ea.status='pendente')::int ),0)  AS pendentes,
              COALESCE(SUM( (ea.status='pausado')::int ),0)   AS pausados,
              COALESCE(SUM( (ea.status='cancelado')::int ),0) AS cancelados,
              COALESCE(SUM( (ea.status='enviado')::int ),0)   AS enviados,
              COALESCE(SUM( (ea.status='erro')::int ),0)      AS erros,
              COUNT(ea.id) AS total
            FROM envios e
            LEFT JOIN envios_analitico ea ON ea.envio_id = e.id
            GROUP BY e.id
            ORDER BY e.criado_em DESC
        """)
        rows = cur.fetchall()

        # Filtragem em memória (simples) se usuário pediu
        def computa_status_geral(r):
            # Deriva um "status geral" do envio:
            #  - se cancelados == total_pendentes+pausados (todos remanescentes cancelados) -> 'cancelado'
            #  - se enviados == total e erros == 0 -> 'concluido'
            #  - se enviados > 0 e pendentes+pausados > 0 -> 'em_andamento'
            #  - se pausados > 0 e pendentes == 0 -> 'pausado'
            #  - se pendentes > 0 -> 'agendado' (ou 'pendente')
            tot = r["total"] or 0
            pen, pau, can, env, err = r["pendentes"], r["pausados"], r["cancelados"], r["enviados"], r["erros"]
            if tot > 0 and env == tot and err == 0:
                return "concluido"
            if (pen + pau) == 0 and env == 0 and can > 0:
                return "cancelado"
            if env > 0 and (pen + pau) > 0:
                return "em_andamento"
            if pau > 0 and pen == 0:
                return "pausado"
            if pen > 0:
                return "agendado"
            # fallback
            return "imediato" if r["modo_envio"] == "imediato" else "agendado"

        resp = []
        for r in rows:
            if modo and r["modo_envio"] != modo:
                continue
            geral = computa_status_geral(r)
            if status_filtro and status_filtro != geral:
                continue

            resp.append({
                "id": r["id"],
                "nome_disparo": r["nome_disparo"],
                "grupo_trabalho": r["grupo_trabalho"],
                "criado_em": _row_to_iso(r["criado_em"]),
                "modo_envio": r["modo_envio"],
                "data_hora_agendamento": _row_to_iso(r["data_hora_agendamento"]),
                "intervalo_msg": r["intervalo_msg"],
                "tamanho_lote": r["tamanho_lote"],
                "intervalo_lote": r["intervalo_lote"],
                "totais": {
                    "total": r["total"],
                    "pendentes": r["pendentes"],
                    "pausados": r["pausados"],
                    "cancelados": r["cancelados"],
                    "enviados": r["enviados"],
                    "erros": r["erros"],
                },
                "status_geral": geral,
            })
        return jsonify(resp)
    except Exception as e:
        print("❌ /api/envios [GET]:", e)
        return jsonify({"ok": False, "erro": "erro ao listar envios"}), 500
    finally:
        cur.close(); conn.close()


@app.route("/api/envios/<int:envio_id>", methods=["GET"])
def obter_envio(envio_id: int):
    """Retorna o envio e, opcionalmente, contatos (paginado). Use ?with_contatos=1&status=pendente&limit=100&offset=0"""
    with_contatos = request.args.get("with_contatos") == "1"
    status_f = request.args.get("status")
    limit = int(request.args.get("limit") or 200)
    offset = int(request.args.get("offset") or 0)

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, nome_disparo, grupo_trabalho, criado_em, modo_envio, data_hora_agendamento,
                   intervalo_msg, tamanho_lote, intervalo_lote, template, token, phone_id, waba_id
            FROM envios WHERE id=%s
        """, (envio_id,))
        e = cur.fetchone()
        if not e:
            return not_found("Envio não encontrado")

        result = {
            "id": e["id"],
            "nome_disparo": e["nome_disparo"],
            "grupo_trabalho": e["grupo_trabalho"],
            "criado_em": _row_to_iso(e["criado_em"]),
            "modo_envio": e["modo_envio"],
            "data_hora_agendamento": _row_to_iso(e["data_hora_agendamento"]),
            "intervalo_msg": e["intervalo_msg"],
            "tamanho_lote": e["tamanho_lote"],
            "intervalo_lote": e["intervalo_lote"],
            "template": e["template"],
            "phone_id": e["phone_id"],
            "waba_id": e["waba_id"]
        }

        if with_contatos:
            params = [envio_id]
            where = "WHERE envio_id=%s"
            if status_f:
                where += " AND status=%s"
                params.append(status_f)

            cur.execute(f"""
                SELECT id, telefone, conteudo, status, data_hora
                FROM envios_analitico
                {where}
                ORDER BY id
                LIMIT %s OFFSET %s
            """, (*params, limit, offset))
            result["contatos"] = cur.fetchall()

        return jsonify(result)
    except Exception as e:
        print("❌ /api/envios/<id> [GET]:", e)
        return jsonify({"ok": False, "erro": "erro ao obter envio"}), 500
    finally:
        cur.close(); conn.close()


@app.route("/api/envios/<int:envio_id>", methods=["PUT"])
def editar_envio(envio_id: int):
    """
    Edita campos do envio. Corpo JSON pode conter qualquer um dos campos:
      - nome_disparo, grupo_trabalho, modo_envio, data_hora_agendamento,
        intervalo_msg, tamanho_lote, intervalo_lote, template, token, phone_id, waba_id
    Obs.: edição não mexe nos contatos já cadastrados (envios_analitico).
    """
    data = request.get_json(silent=True) or {}
    allowed = ["nome_disparo","grupo_trabalho","modo_envio","data_hora_agendamento",
               "intervalo_msg","tamanho_lote","intervalo_lote","template","token","phone_id","waba_id"]

    sets, params = [], []
    for k in allowed:
        if k in data:
            sets.append(f"{k}=%s")
            # jsonb precisa de json.dumps
            if k == "template" and data[k] is not None and not isinstance(data[k], str):
                params.append(json.dumps(data[k]))
            else:
                params.append(data[k])

    if not sets:
        return bad_request("Nenhum campo para atualizar")

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM envios WHERE id=%s", (envio_id,))
        if not cur.fetchone():
            return not_found("Envio não encontrado")

        cur.execute(f"UPDATE envios SET {', '.join(sets)} WHERE id=%s", (*params, envio_id))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        print("❌ /api/envios/<id> [PUT]:", e)
        return jsonify({"ok": False, "erro": "erro ao editar envio"}), 500
    finally:
        cur.close(); conn.close()


@app.route("/api/envios/<int:envio_id>", methods=["DELETE"])
def excluir_envio(envio_id: int):
    """Exclui o envio e seus contatos (CASCADE já configurado)."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("DELETE FROM envios WHERE id=%s RETURNING id", (envio_id,))
        row = cur.fetchone()
        conn.commit()
        if not row:
            return not_found("Envio não encontrado")
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        print("❌ /api/envios/<id> [DELETE]:", e)
        return jsonify({"ok": False, "erro": "erro ao excluir envio"}), 500
    finally:
        cur.close(); conn.close()


@app.route("/api/envios/<int:envio_id>/pause", methods=["PATCH"])
def pausar_envio(envio_id: int):
    """
    Pausa um envio trocando itens 'pendente' -> 'pausado' no analítico.
    O worker só lê 'pendente', então isso efetivamente pausa.
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("UPDATE envios_analitico SET status='pausado' WHERE envio_id=%s AND status='pendente' RETURNING id", (envio_id,))
        qtd = cur.rowcount
        conn.commit()
        return jsonify({"ok": True, "afetados": qtd})
    except Exception as e:
        conn.rollback()
        print("❌ /api/envios/<id>/pause [PATCH]:", e)
        return jsonify({"ok": False, "erro": "erro ao pausar envio"}), 500
    finally:
        cur.close(); conn.close()


@app.route("/api/envios/<int:envio_id>/resume", methods=["PATCH"])
def retomar_envio(envio_id: int):
    """Retoma um envio trocando 'pausado' -> 'pendente'."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("UPDATE envios_analitico SET status='pendente' WHERE envio_id=%s AND status='pausado' RETURNING id", (envio_id,))
        qtd = cur.rowcount
        conn.commit()
        return jsonify({"ok": True, "afetados": qtd})
    except Exception as e:
        conn.rollback()
        print("❌ /api/envios/<id>/resume [PATCH]:", e)
        return jsonify({"ok": False, "erro": "erro ao retomar envio"}), 500
    finally:
        cur.close(); conn.close()


@app.route("/api/envios/<int:envio_id>/cancel", methods=["PATCH"])
def cancelar_envio(envio_id: int):
    """Cancela um envio trocando 'pendente'/'pausado' -> 'cancelado'."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE envios_analitico
               SET status='cancelado'
             WHERE envio_id=%s
               AND status IN ('pendente','pausado')
            RETURNING id
        """, (envio_id,))
        qtd = cur.rowcount
        conn.commit()
        return jsonify({"ok": True, "afetados": qtd})
    except Exception as e:
        conn.rollback()
        print("❌ /api/envios/<id>/cancel [PATCH]:", e)
        return jsonify({"ok": False, "erro": "erro ao cancelar envio"}), 500
    finally:
        cur.close(); conn.close()


# =========================
# Run
# =========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)

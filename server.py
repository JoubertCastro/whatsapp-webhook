from flask import Flask, request, jsonify
from datetime import datetime, timezone, timedelta
import psycopg2
import psycopg2.extras
import json
import os

# === NOVO (LOGIN) ===
from werkzeug.security import generate_password_hash, check_password_hash
from flask_cors import CORS  # ✅ Importa o CORS

app = Flask(__name__)
CORS(app)  # ✅ Ativa CORS para todas as rotas

# ---------- Config ----------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"
)
VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "meu_token_secreto")

# ---------- Helpers ----------
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mensagens (
            id SERIAL PRIMARY KEY,
            data_hora TIMESTAMP,
            remetente TEXT,
            mensagem TEXT,
            direcao TEXT,
            nome TEXT,
            msg_id TEXT,
            phone_number_id TEXT,          -- ✅ novo campo
            display_phone_number TEXT,     -- ✅ novo campo
            raw JSONB
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS status_mensagens (
            id SERIAL PRIMARY KEY,
            data_hora TIMESTAMP,
            msg_id TEXT,
            recipient_id TEXT,
            status TEXT,
            phone_number_id TEXT,          -- ✅ novo campo
            display_phone_number TEXT,     -- ✅ novo campo
            raw JSONB
        );
    """)

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

    conn.commit()
    cur.close()
    conn.close()
    
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
        "INSERT INTO mensagens (data_hora, remetente, mensagem, direcao, nome, msg_id, phone_number_id, display_phone_number, raw) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (data_hora, remetente, mensagem, "in", nome, msg_id,
         phone_number_id, display_phone_number,
         json.dumps(raw) if raw else None)
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
        "INSERT INTO status_mensagens (data_hora, msg_id, recipient_id, status, phone_number_id, display_phone_number, raw) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (data_hora, msg_id, recipient_id, status,
         phone_number_id, display_phone_number,
         json.dumps(raw))
    )
    conn.commit()
    cur.close()
    conn.close()


init_db()

# ---------- Webhook ----------
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
        entry = data.get("entry", [])[0]
        changes = entry.get("changes", [])[0]
        value = changes.get("value", {})

        # ✅ captura os campos de metadata
        phone_number_id = value.get("metadata", {}).get("phone_number_id")
        display_phone_number = value.get("metadata", {}).get("display_phone_number")

        # --- Mensagens recebidas ---
        for msg in value.get("messages", []):
            remetente = msg.get("from", "desconhecido")
            msg_id = msg.get("id")
            tipo = msg.get("type")
            texto = None

            if tipo == "text":
                texto = msg.get("text", {}).get("body")
            elif tipo == "button":
                botao = msg.get("button", {})
                texto = f"{botao.get('text')} (payload: {botao.get('payload')})"
            else:
                texto = f"[{tipo}]"

            nome = value.get("contacts", [{}])[0].get("profile", {}).get("name")

            salvar_mensagem(
                remetente,
                texto,
                msg_id=msg_id,
                nome=nome,
                timestamp=msg.get("timestamp"),
                raw=msg,
                phone_number_id=phone_number_id,
                display_phone_number=display_phone_number
            )

        # --- Status de mensagens ---
        for st in value.get("statuses", []):
            salvar_status(
                st.get("id"),
                st.get("recipient_id"),
                st.get("status"),
                st,
                st.get("timestamp"),
                phone_number_id=phone_number_id,
                display_phone_number=display_phone_number
            )

    except Exception as e:
        print("❌ Erro ao processar webhook:", e)

    return "EVENT_RECEIVED", 200

# ---------- Teste saúde ----------
@app.route("/api/saude")
def saude():
    return jsonify({"ok": True})

# === NOVO (LOGIN) === Cadastro de usuário
@app.route("/api/cadastrar", methods=["POST"])
def cadastrar():
    data = request.get_json(silent=True) or {}
    nome = (data.get("nome") or "").strip()
    email = (data.get("email") or "").strip().lower()
    senha = data.get("senha") or ""

    if not nome or not email or not senha:
        return jsonify({"ok": False, "erro": "nome, email e senha são obrigatórios"}), 400

    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("SELECT id FROM usuarios WHERE email = %s", (email,))
        if cur.fetchone():
            return jsonify({"ok": False, "erro": "email já cadastrado"}), 409

        cur.execute(
            """
            INSERT INTO usuarios (nome, email, senha, criado_em)
            VALUES (%s, %s, %s, (now()))
            RETURNING id, criado_em
            """,
            (nome, email, generate_password_hash(senha))
        )

        row = cur.fetchone()
        conn.commit()
        return jsonify({"ok": True, "id": row["id"], "criado_em": row["criado_em"]})
    except Exception as e:
        conn.rollback()
        print("❌ Erro /api/cadastrar:", e)
        return jsonify({"ok": False, "erro": "erro ao cadastrar usuário"}), 500
    finally:
        cur.close()
        conn.close()

# === NOVO (LOGIN) === Login
@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    senha = data.get("senha") or ""

    if not email or not senha:
        return jsonify({"ok": False, "erro": "email e senha são obrigatórios"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, nome, email, senha, ativo FROM usuarios WHERE email = %s", (email,))
        user = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not user:
        return jsonify({"ok": False, "erro": "credenciais inválidas"}), 401

    if not user["ativo"]:
        return jsonify({"ok": False, "erro": "usuário inativo"}), 403

    if not check_password_hash(user["senha"], senha):
        return jsonify({"ok": False, "erro": "credenciais inválidas"}), 401

    return jsonify({
        "ok": True,
        "user": {
            "id": user["id"],
            "nome": user["nome"],
            "email": user["email"],
            "ativo": user["ativo"]
        }
    })

# === NOVO (ADMINISTRAÇÃO DE USUÁRIOS) ===

@app.route("/api/usuarios", methods=["GET"])
def listar_usuarios():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, nome, email,ativo,criado_em FROM usuarios WHERE ativo = true ORDER BY id")
        usuarios = cur.fetchall()
        return jsonify(usuarios)
    except Exception as e:
        print("❌ Erro /api/usuarios [GET]:", e)
        return jsonify({"ok": False, "erro": "erro ao listar usuários"}), 500
    finally:
        cur.close()
        conn.close()

@app.route("/api/usuarios/<int:id>", methods=["PUT"])
def editar_usuario(id):
    data = request.get_json(silent=True) or {}
    nome = (data.get("nome") or "").strip()
    email = (data.get("email") or "").strip().lower()
    senha = data.get("senha") or ""

    if not nome or not email or not senha:
        return jsonify({"ok": False, "erro": "nome, email e senha são obrigatórios"}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE usuarios SET nome = %s, email = %s, senha = %s WHERE id = %s AND ativo = true",
            (nome, email, generate_password_hash(senha), id)
        )
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        print("❌ Erro /api/usuarios/<id> [PUT]:", e)
        return jsonify({"ok": False, "erro": "erro ao editar usuário"}), 500
    finally:
        cur.close()
        conn.close()

@app.route("/api/usuarios/<int:id>", methods=["DELETE"])
def excluir_usuario(id):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE usuarios SET ativo = false WHERE id = %s", (id,))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        print("❌ Erro /api/usuarios/<id> [DELETE]:", e)
        return jsonify({"ok": False, "erro": "erro ao excluir usuário"}), 500
    finally:
        cur.close()
        conn.close()

@app.route("/api/status", methods=["GET"])
def listar_status():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT data_hora, recipient_id, status, display_phone_number
            FROM (
                SELECT data_hora, recipient_id, status, display_phone_number,
                       row_number() OVER (PARTITION BY recipient_id, msg_id, display_phone_number ORDER BY data_hora DESC) AS atual
                FROM status_mensagens
            ) tb
            WHERE atual = 1
            ORDER BY data_hora DESC
        """)
        rows = cur.fetchall()
        return jsonify(rows)
    except Exception as e:
        print("❌ Erro /api/status:", e)
        return jsonify({"ok": False, "erro": "erro ao buscar status"}), 500
    finally:
        cur.close()
        conn.close()


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




# === Importa e registra conversas ===
from conversas import conversas_bp
app.register_blueprint(conversas_bp)

# ---------- Run ----------


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)

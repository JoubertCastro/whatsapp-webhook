import os, time, json, math, signal, sys
from datetime import datetime
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
import requests
from requests.adapters import HTTPAdapter, Retry

#DATABASE_URL = os.getenv("DATABASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL","postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway")
GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v23.0")

# Tuning via env
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "200"))     # contatos por página
MSG_INTERVAL_S    = float(os.getenv("INTERVALO_MSG", "0.00"))  # pausa entre msgs (segundos)
LOTE_INTERVAL_MIN = float(os.getenv("INTERVALO_LOTE_MIN", "0"))
RATE_LIMIT_RPS    = float(os.getenv("RATE_LIMIT_RPS", "20"))   # req/seg por phone_id
HTTP_TIMEOUT_S    = float(os.getenv("HTTP_TIMEOUT_S", "15"))
RETRY_MAX         = int(os.getenv("RETRY_MAX", "3"))

stop_flag = False
def handle_sigterm(*_):
    global stop_flag
    stop_flag = True
signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

# ---------- POOL DE CONEXÕES ----------
pool = SimpleConnectionPool(minconn=1, maxconn=int(os.getenv("PG_MAXCONN", "10")),
                            dsn=DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor)

def get_conn():
    return pool.getconn()

def put_conn(conn):
    pool.putconn(conn)

# ---------- HTTP SESSION (retry/backoff) ----------
session = requests.Session()
retries = Retry(
    total=RETRY_MAX,
    backoff_factor=1.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=["POST"]
)
session.mount("https://", HTTPAdapter(max_retries=retries))

def enviar_whatsapp(payload, token, phone_id):
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = session.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT_S)
        ok = resp.ok
        body = resp.json() if "application/json" in resp.headers.get("Content-Type", "") else {"raw": resp.text}
        return ok, body, resp.status_code
    except requests.RequestException as e:
        return False, {"error": str(e)}, 0

def montar_payload(template, contato):
    components = []

    # HEADER
    if template.get("headerType") == "TEXT" and template.get("headerVars", 0) > 0:
        vals = []
        try:
            mapping = template.get("mapping", {}).get("headerText", [])
            parts = (contato.get("conteudo") or "").split(",")
            for i in mapping:
                vals.append(parts[i] if i < len(parts) else "")
        except Exception:
            vals = []
        if vals:
            components.append({"type": "header",
                               "parameters": [{"type": "text", "text": str(v)} for v in vals]})

    elif template.get("headerType") in ["IMAGE", "VIDEO", "DOCUMENT"]:
        link = template.get("mediaLink")
        if link:
            key = template["headerType"].lower()
            components.append({"type":"header",
                               "parameters":[{ "type": key, key: {"link": link}}]})

    # BODY
    if template.get("bodyVars", 0) > 0:
        vals = []
        try:
            mapping = template.get("mapping", {}).get("body", [])
            parts = (contato.get("conteudo") or "").split(",")
            for i in mapping:
                vals.append(parts[i] if i < len(parts) else "")
        except Exception:
            vals = []
        if vals:
            components.append({"type":"body",
                               "parameters":[{"type":"text","text":str(v)} for v in vals]})

    # BUTTONS URL
    for btn in template.get("urlButtons", []):
        if btn.get("hasVar"):
            mapArr = template.get("mapping", {}).get("urlButtons", {}).get(str(btn["index"]), [])
            if mapArr:
                parts = (contato.get("conteudo") or "").split(",")
                v = parts[mapArr[0]] if mapArr[0] < len(parts) else ""
                components.append({
                    "type":"button", "sub_type":"url", "index":str(btn["index"]),
                    "parameters":[{"type":"text","text":str(v)}]
                })

    return {
        "messaging_product": "whatsapp",
        "to": contato["telefone"],
        "type": "template",
        "template": {
            "name": template["name"],
            "language": {"code": template.get("language","pt_BR")},
            "components": components
        }
    }

# ---------- CLAIM DE ENVIO COM LOCK ----------
def claim_envio():
    """
    Seleciona 1 envio pronto e marca como processando, de forma atômica.
    Requer colunas: status (pendente|processando|concluido), criado_em, modo_envio, data_hora_agendamento timestamptz
    """
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                WITH picked AS (
                  SELECT id
                  FROM envios
                  WHERE status = 'pendente'
                    AND (
                      modo_envio = 'imediato'
                      OR (modo_envio = 'agendar' AND data_hora_agendamento <= NOW())
                    )
                  ORDER BY criado_em ASC
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                UPDATE envios e
                   SET status = 'processando', iniciado_em = NOW()
                FROM picked p
                WHERE e.id = p.id
                RETURNING e.*;
            """)
            row = cur.fetchone()
            return row
    finally:
        put_conn(conn)

def finalizar_envio(envio_id, ok=True):
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                UPDATE envios
                   SET status = %s,
                       finalizado_em = NOW()
                 WHERE id = %s
            """, ('concluido' if ok else 'erro', envio_id))
    finally:
        put_conn(conn)

def fetch_contatos_pagina(envio_id, limit, offset):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, telefone, conteudo
                  FROM envios_analitico
                 WHERE envio_id = %s AND status = 'pendente'
                 ORDER BY id
                 LIMIT %s OFFSET %s
            """, (envio_id, limit, offset))
            return cur.fetchall()
    finally:
        put_conn(conn)

def marcar_status_batch(pares_id_status):
    if not pares_id_status:
        return
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                "UPDATE envios_analitico SET status = %s, atualizado_em = NOW(), detalhe = %s WHERE id = %s",
                [(st, json.dumps(det or {}), _id) for (_id, st, det) in pares_id_status],
                page_size=500
            )
    finally:
        put_conn(conn)

def processar_envio(envio):
    envio_id   = envio["id"]
    template   = envio["template"] if isinstance(envio["template"], dict) else json.loads(envio["template"])
    token      = envio.get("token")
    phone_id   = envio.get("phone_id")

    # rate limit por phone_id
    sleep_between = 1.0 / RATE_LIMIT_RPS if RATE_LIMIT_RPS > 0 else 0.0

    # quantos pendentes existem?
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM envios_analitico WHERE envio_id=%s AND status='pendente'", (envio_id,))
            total = cur.fetchone()["n"]
    finally:
        put_conn(conn)

    enviados = 0
    paginas = math.ceil(total / BATCH_SIZE) if total else 0

    for p in range(paginas):
        if stop_flag: break
        offset = p * BATCH_SIZE
        contatos = fetch_contatos_pagina(envio_id, BATCH_SIZE, offset)

        updates = []
        for c in contatos:
            payload = montar_payload(template, c)
            ok, body, status_code = enviar_whatsapp(payload, token, phone_id)

            detalhe = {}
            if ok:
                # Tente capturar o message_id retornado pela Meta
                try:
                    entries = body.get("messages", [])
                    if entries and "id" in entries[0]:
                        detalhe["message_id"] = entries[0]["id"]
                except Exception:
                    pass
            else:
                detalhe = {"error": body, "http_status": status_code}

            updates.append((c["id"], "enviado" if ok else "erro", detalhe))
            enviados += 1

            if MSG_INTERVAL_S > 0:
                time.sleep(MSG_INTERVAL_S)
            elif sleep_between > 0:
                time.sleep(sleep_between)

        # flush da página
        marcar_status_batch(updates)

        if LOTE_INTERVAL_MIN > 0 and (p + 1) < paginas:
            time.sleep(LOTE_INTERVAL_MIN * 60.0)

    finalizar_envio(envio_id, ok=True)
    print(f"🏁 Envio {envio_id} concluído ({enviados}/{total})")

def main():
    print("🚀 Worker (envios) pronto.")
    while not stop_flag:
        envio = claim_envio()
        if envio:
            try:
                processar_envio(envio)
            except Exception as e:
                # Marca erro no envio e segue; evita crash geral
                try:
                    finalizar_envio(envio["id"], ok=False)
                except Exception:
                    pass
                print("❌ Falha no processamento:", e)
                time.sleep(2)
        else:
            time.sleep(3)

if __name__ == "__main__":
    main()

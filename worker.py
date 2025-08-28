import time
import json
import os
from datetime import datetime, timezone
import psycopg2
import psycopg2.extras
import requests

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:MHKRBuSTXcoAfNhZNErtPnCaLySHHlPd@postgres.railway.internal:5432/railway"
)

# 🔑 Pega do ambiente para não expor no banco
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
PHONE_ID = os.getenv("PHONE_ID")
GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v23.0")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def fetch_pendentes():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM envios e
        WHERE 
            (modo_envio = 'imediato' OR (modo_envio = 'agendar' AND data_hora_agendamento <= NOW()))
            AND EXISTS (
                SELECT 1 FROM envios_analitico ea
                WHERE ea.envio_id = e.id AND ea.status = 'pendente'
            )
        ORDER BY criado_em ASC
        LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def fetch_contatos(envio_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM envios_analitico WHERE envio_id = %s AND status = 'pendente'", (envio_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def atualizar_status(contato_id, status):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE envios_analitico SET status = %s WHERE id = %s", (status, contato_id))
    conn.commit()
    cur.close()
    conn.close()

def montar_payload(template, contato):
    components = []

    # HEADER
    if template.get("headerType") == "TEXT" and template.get("headerVars", 0) > 0:
        vals = [contato["conteudo"].split(",")[i] for i in template["mapping"]["headerText"]]
        components.append({
            "type": "header",
            "parameters": [{"type": "text", "text": str(v)} for v in vals]
        })
    elif template.get("headerType") in ["IMAGE","VIDEO","DOCUMENT"]:
        link = template.get("mediaLink")
        if link:
            key = template["headerType"].lower()
            components.append({
                "type":"header",
                "parameters":[{ "type": key, key: {"link": link}}]
            })

    # BODY
    if template.get("bodyVars", 0) > 0:
        vals = [contato["conteudo"].split(",")[i] for i in template["mapping"]["body"]]
        components.append({
            "type":"body",
            "parameters":[{"type":"text","text":str(v)} for v in vals]
        })

    # BUTTONS URL
    for btn in template.get("urlButtons", []):
        if btn.get("hasVar"):
            mapArr = template["mapping"]["urlButtons"].get(str(btn["index"]), [])
            if mapArr:
                v = contato["conteudo"].split(",")[mapArr[0]]
                components.append({
                    "type":"button",
                    "sub_type":"url",
                    "index":str(btn["index"]),
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


def enviar_whatsapp(payload):
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    return r.ok, r.text


def processar_envio(envio):
    template = envio.get("template")  # já vem do banco (JSONB)
    contatos = fetch_contatos(envio["id"])
    tamanho_lote = envio.get("tamanho_lote") or len(contatos)
    intervalo_lote = envio.get("intervalo_lote") or 0
    intervalo_msg = envio.get("intervalo_msg") or 0

    total = len(contatos)
    enviados = 0
    for i in range(0, total, tamanho_lote):
        lote = contatos[i:i+tamanho_lote]
        for c in lote:
            payload = montar_payload(template, c)
            ok, resp = enviar_whatsapp(payload)
            print("✔️" if ok else "❌", c["telefone"], resp[:100])

            atualizar_status(c["id"], "enviado" if ok else "erro")
            enviados += 1

            if intervalo_msg > 0:
                time.sleep(intervalo_msg)

        if i + tamanho_lote < total and intervalo_lote > 0:
            print(f"⏱️ Esperando {intervalo_lote} min antes do próximo lote...")
            time.sleep(intervalo_lote * 60)

    print(f"🏁 Envio {envio['id']} concluído ({enviados}/{total})")


def main():
    print("🚀 Worker iniciado. Aguardando envios...")
    while True:
        envio = fetch_pendentes()
        if envio:
            processar_envio(envio)
        else:
            time.sleep(10)  # nada a fazer, espera 10s


if __name__ == "__main__":
    main()

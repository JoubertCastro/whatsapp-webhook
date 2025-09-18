from fastapi import FastAPI
from sqlalchemy import create_engine, text
import os

app = FastAPI()

# Função para abrir conexão com o SQL Server

conn_str = f"mssql+pytds://{os.environ['DB_USER']}:{os.environ['DB_PASS']}@{os.environ['DB_SERVER']}/{os.environ['DB_NAME']}"
engine = create_engine(conn_str)

# Endpoint que lista os 10 primeiros registros
@app.get("/cadastro")
def listar_cadastros():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT TOP 10 * FROM sic.dbo.cadastro"))
        rows = [dict(row) for row in result]
    return rows

# Endpoint dinâmico: busca por codigo_interno
@app.get("/cadastro/{id}")
def get_cadastro_by_id(id: int):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM sic.dbo.cadastro WHERE codigo_interno = :id"), {"id": id})
        row = result.fetchone()
        if row:
            return dict(row)
        return {"error": "Cadastro não encontrado"}


# Endpoint dinâmico: busca por agentes
@app.get("/agentes/{id}")
def get_agente_by_id(id: int):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM sic.dbo.agentes WHERE codigo_do_agente = :id"), {"id": id})
        row = result.fetchone()
        if row:
            return dict(row)
        return {"error": "Cadastro não encontrado"}

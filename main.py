from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Optional, List
from datetime import date
import json

from auth import create_access_token, verify_token
from database import get_connection

# consultas já existentes
from queries.consulta_clientes import get_dados_clientes
from queries.consulta_lojas import get_dados_lojas
from queries.consulta_sensores import get_dados_sensores
from queries.consulta_estoque import get_dados_estoque
from queries.consulta_estoque_detalhes import get_dados_estoque_detalhes
from queries.consulta_chamados import get_dados_chamados
from queries.consulta_users import get_dados_users

# consultas de INTEGRAÇÃO
from queries.gaps_full import get_gaps_full     # full dataset
from queries.integracao_ok import get_integracao_ok                  # somente OK

app = FastAPI()

# CORS liberado para Lovable / Seed
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://chamados-dev.web.app",
        "https://chamados.dev.seeddigital.com.br",
        "https://chamados-dev-seed.web.app",
        "https://painel.seeddigital.com.br",
    ],
    allow_origin_regex=r"https://.*\.lovableproject\.com",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------
# AUTH JWT
# ------------------------------------------------
@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if form_data.username == "admin" and form_data.password == "admin123":
        access_token = create_access_token(data={"sub": form_data.username})
        return {"access_token": access_token, "token_type": "bearer"}
    else:
        raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")


# ------------------------------------------------
# ENDPOINTS EXISTENTES
# ------------------------------------------------
@app.get("/clientes")
def clientes(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_dados_clientes(conn)

@app.get("/lojas")
def lojas(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_dados_lojas(conn)

@app.get("/sensores")
def sensores(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_dados_sensores(conn)

@app.get("/estoque")
def estoque(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_dados_estoque(conn)

@app.get("/estoque_detalhes")
def estoque(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_dados_estoque_detalhes(conn)

@app.get("/chamados")
def chamados(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_dados_chamados(conn)

@app.get("/users")
def users(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_dados_users(conn)


# ------------------------------------------------
# NOVOS ENDPOINTS DE INTEGRAÇÃO
# ------------------------------------------------

# FULL — retorna todos os horários de integração ( OK + GAP )
@app.get("/gaps_full")
def gaps_full(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_gaps_full(conn)

# Apenas OK — TICKET NOT NULL
@app.get("/integracao_ok")
def integracao_ok(
    token: dict = Depends(verify_token),
    data: Optional[str] = None,
    site_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 5000
):
    conn = get_connection()

    offset = (page - 1) * page_size

    return get_integracao_ok(
        conn,
        data=data,
        site_id=site_id,
        offset=offset,
        limit=page_size
    )



# ------------------------------------------------
# ROOT
# ------------------------------------------------
@app.get("/")
def root():
    return {"msg": "API rodando com sucesso 🚀"}


# ----------------------------------------
# ROTA: POST /chamado
# ----------------------------------------
class Chamado(BaseModel):
    cliente: str
    responsavel: str
    titulo: str
    problema: str
    impacto: str
    urgencia: bool
    detalhe_urgencia: Optional[str] = None
    prazo: Optional[date] = None
    relevancia: str
    anexos: Optional[List[str]] = []
    trello_card_url: Optional[str] = None
    usuario_id: int

@app.post("/chamado")
def criar_chamado(chamado: Chamado, token: dict = Depends(verify_token)):
    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO DS_CHAMADOS_DEV (
            cliente, responsavel, titulo, problema, impacto,
            urgencia, detalhe_urgencia, prazo, relevancia,
            anexos, data_criacao, trello_card_url, usuario_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)
    """

    cursor.execute(
        insert_query,
        (
            chamado.cliente,
            chamado.responsavel,
            chamado.titulo,
            chamado.problema,
            chamado.impacto,
            chamado.urgencia,
            chamado.detalhe_urgencia,
            chamado.prazo,
            chamado.relevancia,
            json.dumps(chamado.anexos),
            chamado.trello_card_url,
            chamado.usuario_id
        )
    )

    conn.commit()
    cursor.close()
    conn.close()

    return {"message": "Chamado registrado com sucesso ✅"}


# ----------------------
# GET /chamado/{usuario_id}
# ----------------------
@app.get("/chamado/{usuario_id}")
def listar_chamados_usuario(usuario_id: int, token: dict = Depends(verify_token)):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT id, cliente, responsavel, titulo, problema, impacto,
               urgencia, detalhe_urgencia, prazo, relevancia,
               anexos, data_criacao, trello_card_url, status
        FROM DS_CHAMADOS_DEV
        WHERE usuario_id = ?
        ORDER BY data_criacao DESC
    """

    cursor.execute(query, (usuario_id,))
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    chamados = []
    
    for row in rows:
        chamado = dict(zip(columns, row))
        if chamado.get("anexos"):
            try:
                chamado["anexos"] = json.loads(chamado["anexos"])
            except:
                chamado["anexos"] = []
        chamados.append(chamado)

    cursor.close()
    conn.close()

    return chamados

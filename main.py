from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Optional, List
from datetime import date
import json

from auth import create_access_token, verify_token
from database import get_connection

# ------------------------------------------------
# CONSULTAS JÁ EXISTENTES
# ------------------------------------------------
from queries.consulta_clientes import get_dados_clientes
from queries.consulta_lojas import get_dados_lojas
from queries.consulta_sensores import get_dados_sensores
from queries.consulta_estoque import get_dados_estoque
from queries.consulta_estoque_detalhes import get_dados_estoque_detalhes
from queries.consulta_chamados import get_dados_chamados
from queries.consulta_users import get_dados_users

# COMPANY
from queries.consulta_company import get_company

# INTEGRAÇÃO
from queries.gaps_full import get_gaps_full
from queries.integracao_ok import get_integracao_ok

# 🔹 NOVAS CONSULTAS (SENSORES)
from queries.consulta_sensores_instalados import get_sensores_instalados
from queries.consulta_sensores_desinstalados import get_sensores_desinstalados

# ROUTERS
from queries.briefing import router as briefing_router
from queries.nucleo import router as nucleo_router
from queries.segmento import router as segmento_router

app = FastAPI()

# ------------------------------------------------
# CORS
# ------------------------------------------------
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
# MODELOS
# ------------------------------------------------
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


class CompanyCreate(BaseModel):
    DS_COMPANY_DESCRIPTION: Optional[str] = None
    DS_COMPANY_EMPRESA_ID: Optional[int] = None
    DS_STATUS: Optional[str] = None
    DS_COMPANY_SENHA_INTEGRACAO: Optional[str] = None


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
def estoque_detalhes(token: dict = Depends(verify_token)):
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
# 🔹 NOVOS ENDPOINTS – SENSORES
# ------------------------------------------------
@app.get("/sensores/instalados")
def sensores_instalados(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_sensores_instalados(conn)


@app.get("/sensores/desinstalados")
def sensores_desinstalados(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_sensores_desinstalados(conn)

# ------------------------------------------------
# COMPANY
# ------------------------------------------------
@app.get("/company")
def company(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_company(conn)


@app.post("/company")
def criar_company(dados: CompanyCreate, token: dict = Depends(verify_token)):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO DS_COMPANY (
            DS_COMPANY_DESCRIPTION,
            DS_COMPANY_EMPRESA_ID,
            DS_STATUS,
            DS_COMPANY_SENHA_INTEGRACAO
        )
        VALUES (?, ?, ?, ?);

        SELECT SCOPE_IDENTITY() AS DS_COMPANY_BS_ID;
    """

    cursor.execute(
        query,
        (
            dados.DS_COMPANY_DESCRIPTION,
            dados.DS_COMPANY_EMPRESA_ID,
            dados.DS_STATUS,
            dados.DS_COMPANY_SENHA_INTEGRACAO,
        )
    )

    # 🔑 ESSENCIAL no pyodbc
    cursor.nextset()
    new_id = cursor.fetchone()[0]

    conn.commit()
    cursor.close()
    conn.close()

    return {
        "message": "Company criada com sucesso 🚀",
        "DS_COMPANY_BS_ID": int(new_id)
    }

# ------------------------------------------------
# INTEGRAÇÃO
# ------------------------------------------------
@app.get("/gaps_full")
def gaps_full(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_gaps_full(conn)


@app.get("/integracao_ok")
def integracao_ok(
    token: dict = Depends(verify_token),
    data: Optional[str] = None,
    site_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 5000,
):
    conn = get_connection()
    offset = (page - 1) * page_size

    return get_integracao_ok(
        conn,
        data=data,
        site_id=site_id,
        offset=offset,
        limit=page_size,
    )

# ------------------------------------------------
# ROUTERS
# ------------------------------------------------
app.include_router(briefing_router)
app.include_router(nucleo_router)
app.include_router(segmento_router)

# ------------------------------------------------
# ROOT
# ------------------------------------------------
@app.get("/")
def root():
    return {"msg": "API rodando com sucesso 🚀"}

# ------------------------------------------------
# CHAMADOS
# ------------------------------------------------
@app.post("/chamado")
def criar_chamado(chamado: Chamado, token: dict = Depends(verify_token)):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO DS_CHAMADOS_DEV (
            cliente, responsavel, titulo, problema, impacto,
            urgencia, detalhe_urgencia, prazo, relevancia,
            anexos, data_criacao, trello_card_url, usuario_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)
    """

    cursor.execute(
        query,
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
        chamado.usuario_id,
    )

    conn.commit()
    cursor.close()
    conn.close()

    return {"message": "Chamado registrado com sucesso ✅"}


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
    columns = [col[0] for col in cursor.description]

    chamados = []
    for row in rows:
        chamado = dict(zip(columns, row))
        try:
            chamado["anexos"] = json.loads(chamado["anexos"]) if chamado["anexos"] else []
        except Exception:
            chamado["anexos"] = []
        chamados.append(chamado)

    cursor.close()
    conn.close()

    return chamados

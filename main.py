from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from auth import create_access_token, verify_token
from database import get_connection
from queries.consulta_clientes import get_dados_clientes
from queries.consulta_lojas import get_dados_lojas
from queries.consulta_sensores import get_dados_sensores
from queries.consulta_estoque import get_dados_estoque
from queries.consulta_estoque_detalhes import get_dados_estoque_detalhes
from queries.consulta_chamados import get_dados_chamados


app = FastAPI()

# Domínios permitidos para CORS (incluir todos os usados no Lovable)
origins = [
    "https://f1caee16-06fd-4a62-877f-325cc7fad0eb.lovableproject.com",
    "https://preview--painel-seed.lovable.app",
    "https://painel-seed.lovable.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # não pode ser "*"
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if form_data.username == "admin" and form_data.password == "admin123":
        access_token = create_access_token(data={"sub": form_data.username})
        return {"access_token": access_token, "token_type": "bearer"}
    else:
        raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")



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

@app.get("/estoque_chamados")
def estoque(token: dict = Depends(verify_token)):
    conn = get_connection()
    return get_dados_chamados(conn)



@app.get("/")
def root():
    return {"msg": "API rodando com sucesso 🚀"}

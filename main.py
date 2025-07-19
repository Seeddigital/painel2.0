from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware  # ✅ Importar o middleware

from auth import create_access_token, verify_token
from database import get_connection
from queries.consulta_clientes import get_dados_clientes

app = FastAPI()

# ✅ Middleware de CORS — ANTES das rotas
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ⚠️ Use ["*"] apenas para testes. Depois especifique seu domínio.
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

@app.get("/")
def root():
    return {"msg": "API rodando com sucesso 🚀"}


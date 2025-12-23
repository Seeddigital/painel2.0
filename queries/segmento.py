from fastapi import APIRouter, Depends
from pydantic import BaseModel
from database import get_connection
from auth import verify_token

router = APIRouter(prefix="/segmentos", tags=["Segmentos"])

# -------------------------
# MODELOS
# -------------------------
class SegmentoCreate(BaseModel):
    ds_segmento_description: str

# -------------------------
# GET
# -------------------------
@router.get("", dependencies=[Depends(verify_token)])
def get_segmentos():
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT 
            DS_SEGMENTO_ID,
            DS_SEGMENTO_DESCRIPTION
        FROM [Seed_CFG_Analytics].[dbo].[DS_SEGMENTO]
        WHERE DS_STATUS = 1
        ORDER BY DS_SEGMENTO_DESCRIPTION
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]

# -------------------------
# POST
# -------------------------
@router.post("", dependencies=[Depends(verify_token)])
def create_segmento(data: SegmentoCreate):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO [Seed_CFG_Analytics].[dbo].[DS_SEGMENTO]
        (
            DS_SEGMENTO_DESCRIPTION,
            DS_STATUS
        )
        VALUES (?, 1)
    """

    cursor.execute(query, data.ds_segmento_description)
    conn.commit()

    return {"status": "ok", "message": "Segmento criado com sucesso"}

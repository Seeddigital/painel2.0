from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List
from database import get_connection
from auth import verify_token

router = APIRouter(prefix="/nucleos", tags=["Núcleos"])

# -------------------------
# MODELOS
# -------------------------
class NucleoCreate(BaseModel):
    ds_nucleo_description: str
    ds_nucleo_company_id_bs: int
    ds_nucleo_segmento_id: int

# -------------------------
# GET
# -------------------------
@router.get("", dependencies=[Depends(verify_token)])
def get_nucleos():
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT 
            a.DS_NUCLEO_ID,
            a.DS_NUCLEO_DESCRIPTION,
            b.DS_COMPANY_DESCRIPTION,
            a.DS_NUCLEO_SEGMENTO_ID,
            c.DS_SEGMENTO_DESCRIPTION
        FROM [Seed_CFG_Analytics].[dbo].[DS_NUCLEO] a
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_COMPANY] b
            ON a.DS_NUCLEO_COMPANY_ID_BS = b.DS_COMPANY_BS_ID
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_SEGMENTO] c
            ON c.DS_SEGMENTO_ID = a.DS_NUCLEO_SEGMENTO_ID
        WHERE a.DS_STATUS = 1
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]

# -------------------------
# POST
# -------------------------
@router.post("", dependencies=[Depends(verify_token)])
def create_nucleo(data: NucleoCreate):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO [Seed_CFG_Analytics].[dbo].[DS_NUCLEO]
        (
            DS_NUCLEO_DESCRIPTION,
            DS_NUCLEO_COMPANY_ID_BS,
            DS_NUCLEO_SEGMENTO_ID,
            DS_STATUS
        )
        VALUES (?, ?, ?, 1)
    """

    cursor.execute(
        query,
        data.ds_nucleo_description,
        data.ds_nucleo_company_id_bs,
        data.ds_nucleo_segmento_id
    )

    conn.commit()
    return {"status": "ok", "message": "Núcleo criado com sucesso"}

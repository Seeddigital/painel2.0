from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import List, Optional
import json
from database import get_connection
from auth import verify_token

router = APIRouter()

# -------------------------
# MODELOS Pydantic
# -------------------------

class Sponsor(BaseModel):
    email: str
    name: str
    phone: Optional[str]
    position: Optional[str]

class Contact(BaseModel):
    email: Optional[str]
    name: Optional[str]
    phone: Optional[str]
    cellphone: Optional[str]
    position: Optional[str]

class FinancialContact(Contact):
    pass

class ScheduleContact(Contact):
    pass

class AccessPoint(BaseModel):
    accessNumber: int
    height: str
    width: str

class Schedules(BaseModel):
    saturday: Optional[str]
    sunday: Optional[str]
    weekdays: Optional[str]

class Connectivity(BaseModel):
    type: Optional[str]
    wifiNetwork: Optional[str]
    wifiPassword: Optional[str]

class ProjectInfo(BaseModel):
    client: str
    clientStatus: str
    dashboard: str
    dataAccess: str
    isPOC: str
    salesperson: str
    scopeMeetingRequired: str
    sponsor: Sponsor
    totalSensors: int
    totalStores: int
    type: str

class StoreInfo(BaseModel):
    accessPoints: List[AccessPoint]
    address: str
    ceilingHeight: str
    cep: str
    city: str
    cnpj: str
    companyName: str
    connectivity: Connectivity

    # contatos separados
    storeContact: Contact
    scheduleContact: ScheduleContact
    financialContact: FinancialContact

    dataType: str
    schedules: Schedules
    sensors: int
    state: str
    storeCode: int
    tags: Optional[str]

class Store(BaseModel):
    name: str
    observations: List[str]
    projectInfo: ProjectInfo
    storeInfo: StoreInfo

    # campos já existentes
    trello_card_url: Optional[str] = Field(default=None, alias="trello_card_url")
    user: Optional[str] = None

    # núcleo / segmento
    ds_nucleo_id: Optional[int] = None
    ds_nucleo_segmento_id: Optional[int] = None
    ds_segmento_description: Optional[str] = None

class BriefingRequest(BaseModel):
    stores: List[Store]

# -------------------------
# ENDPOINT
# -------------------------
@router.post("/briefing")
def create_briefing(data: BriefingRequest, token=Depends(verify_token)):
    conn = get_connection()
    cursor = conn.cursor()

    for store in data.stores:
        si = store.storeInfo
        pi = store.projectInfo

        query = """
        INSERT INTO DS_BRIEFING (
            STORE_NAME, STORE_CODE, SENSORS, ADDRESS, CITY, STATE, CEP, CNPJ,
            COMPANY_NAME, CEILING_HEIGHT, DATA_TYPE,

            PROJECT_CLIENT, PROJECT_CLIENT_STATUS, PROJECT_DASHBOARD, PROJECT_DATA_ACCESS,
            PROJECT_IS_POC, PROJECT_SALESPERSON, PROJECT_SCOPE_MEETING, PROJECT_TYPE,
            PROJECT_TOTAL_SENSORS, PROJECT_TOTAL_STORES,

            SPONSOR_NAME, SPONSOR_EMAIL, SPONSOR_PHONE, SPONSOR_POSITION,

            STORE_CONTACT_NAME, STORE_CONTACT_EMAIL, STORE_CONTACT_PHONE, STORE_CONTACT_CELLPHONE, STORE_CONTACT_POSITION,

            FIN_CONTACT_NAME, FIN_CONTACT_EMAIL, FIN_CONTACT_PHONE, FIN_CONTACT_CELLPHONE, FIN_CONTACT_POSITION,

            SCHEDULE_CONTACT_NAME, SCHEDULE_CONTACT_EMAIL, SCHEDULE_CONTACT_PHONE,
            SCHEDULE_CONTACT_CELLPHONE, SCHEDULE_CONTACT_POSITION,

            SCHEDULE_WEEKDAYS, SCHEDULE_SATURDAY, SCHEDULE_SUNDAY,

            OBSERVATIONS, ACCESS_POINTS, TAGS,
            trelloCardUrl, [User],
            DS_NUCLEO_ID, DS_NUCLEO_SEGMENTO_ID, DS_SEGMENTO_DESCRIPTION
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?, ?
        )
        """

        cursor.execute(query, (
            # loja
            store.name,
            si.storeCode,
            si.sensors,
            si.address,
            si.city,
            si.state,
            si.cep,
            si.cnpj,
            si.companyName,
            si.ceilingHeight,
            si.dataType,

            # projeto
            pi.client,
            pi.clientStatus,
            pi.dashboard,
            pi.dataAccess,
            pi.isPOC,
            pi.salesperson,
            pi.scopeMeetingRequired,
            pi.type,
            pi.totalSensors,
            pi.totalStores,

            # sponsor
            pi.sponsor.name,
            pi.sponsor.email,
            pi.sponsor.phone,
            pi.sponsor.position,

            # contato loja
            si.storeContact.name,
            si.storeContact.email,
            si.storeContact.phone,
            si.storeContact.cellphone,
            si.storeContact.position,

            # contato financeiro
            si.financialContact.name,
            si.financialContact.email,
            si.financialContact.phone,
            si.financialContact.cellphone,
            si.financialContact.position,

            # contato agendamento
            si.scheduleContact.name,
            si.scheduleContact.email,
            si.scheduleContact.phone,
            si.scheduleContact.cellphone,
            si.scheduleContact.position,

            # horários
            si.schedules.weekdays,
            si.schedules.saturday,
            si.schedules.sunday,

            # json
            json.dumps(store.observations),
            json.dumps([ap.dict() for ap in si.accessPoints]),
            si.tags,

            # extras
            store.trello_card_url,
            store.user,

            # núcleo
            store.ds_nucleo_id,
            store.ds_nucleo_segmento_id,
            store.ds_segmento_description,
        ))

    conn.commit()
    conn.close()

    return {"status": "success", "message": "Briefing cadastrado com sucesso!"}



def get_dados_chamados(conn):
    query = """ 
SELECT
    A.DS_CHAMADO_ID,
    A.DS_CHAMADO_TIPO,
    A.DS_CHAMADO_STATUS,
    C.DS_COMPANY_DESCRIPTION,
    B.DS_SITE_DESCRIPTION,
    A.DS_CHAMADO_SENSOR_ID,
    D.DS_TECNICO_NOME,
    A.DS_CHAMADO_VALOR,
    A.DS_CHAMADO_DT_ABERTURA,
    A.DS_CHAMADO_DT_AGENDAMENTO

FROM [Seed_CFG_Analytics].[dbo].[DS_CHAMADOS] A
INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_SITE] B ON A.DS_CHAMADO_SITE_ID = B.DS_SITE_BS_ID
INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_COMPANY] C ON B.DS_SITE_COMPANY_ID_BS = C.DS_COMPANY_BS_ID
INNER JOIN [Seed_CFG_Analytics].[dbo].DS_TECNICOS D ON A.DS_CHAMADO_TECNICO_ID = D.DS_TECNICO_ID


 """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return results

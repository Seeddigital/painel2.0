
def get_dados_sensores(conn):
    query = """ 

SELECT 
    C.DS_COMPANY_DESCRIPTION AS CLIENTE,
    COUNT(DISTINCT B.DS_SITE_SENSORES_ID) AS SENSORES,
    G.DS_FABRICANTES_DESCRIPTION AS FABRICANTE, 
    F.DS_TIPO_SENSOR_DESCRIPTION AS MODELO,
    D.DS_DATA_SOURCE_PK AS PK, 
    E.DS_SENSORES_MAC AS MAC

FROM 
    [Seed_CFG_Analytics].[dbo].[DS_SITE] A 
INNER JOIN 
    [Seed_CFG_Analytics].[dbo].DS_SITE_SENSORES B 
    ON A.DS_SITE_BS_ID = B.DS_SITE_SENSORES_SITE_ID
INNER JOIN 
    [Seed_CFG_Analytics].[dbo].DS_COMPANY C 
    ON C.DS_COMPANY_BS_ID = A.DS_SITE_COMPANY_ID_BS
INNER JOIN 
    [Seed_CFG_Analytics].[dbo].DS_SENSORES E 
    ON E.DS_SENSORES_ID = B.DS_SITE_SENSORES_SENSOR_ID
INNER JOIN 
    [Seed_CFG_Analytics].[dbo].DS_TIPO_SENSOR F 
    ON E.DS_SENSORES_TIPO_SENSOR_ID = F.DS_TIPO_SENSOR_ID
INNER JOIN 
    [Seed_CFG_Analytics].[dbo].DS_FABRICANTES G 
    ON F.DS_TIPO_SENSOR_FABRICANTE_ID = G.DS_FABRICANTES_ID
LEFT JOIN 
    [Seed_CFG_Analytics].[dbo].DS_DATA_SOURCE D 
    ON D.DS_DATA_SOURCE_SENSOR_ID = E.DS_SENSORES_ID 
    AND D.DS_STATUS = 1 
    AND D.DS_DATA_DESATIVACAO IS NULL

WHERE 
    A.DS_STATUS = 1 AND 
    B.DS_STATUS = 1 AND 
    A.DS_SITE_COMPANY_ID_BS = 20

GROUP BY 
    C.DS_COMPANY_DESCRIPTION,
    G.DS_FABRICANTES_DESCRIPTION,
    F.DS_TIPO_SENSOR_DESCRIPTION,    
    D.DS_DATA_SOURCE_PK, 
    E.DS_SENSORES_MAC;




 """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return results

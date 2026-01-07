def get_sensores_instalados(conn):
    cursor = conn.cursor()

    query = """
    SELECT 
        C.DS_COMPANY_DESCRIPTION AS CLIENTE,
        A.DS_SITE_DESCRIPTION AS LOJA,
        D.DS_DATA_SOURCE_DATA_INICIO AS DATA_INICIO,
        COUNT(DISTINCT D.DS_DATA_SOURCE_SENSOR_ID) AS QTD_SENSORES
    FROM 
        [Seed_CFG_Analytics].[dbo].[DS_SITE] A
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_SITE_SENSORES] B 
            ON A.DS_SITE_BS_ID = B.DS_SITE_SENSORES_SITE_ID
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_COMPANY] C 
            ON C.DS_COMPANY_BS_ID = A.DS_SITE_COMPANY_ID_BS
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_DATA_SOURCE] D 
            ON A.DS_SITE_BS_ID = D.DS_DATA_SOURCE_SITE_ID
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_TIPO_METRICA] E 
            ON D.DS_DATA_SOURCE_TM_ID = E.DS_TIPO_METRICA_ID
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_SENSORES] F 
            ON B.DS_SITE_SENSORES_SENSOR_ID = F.DS_SENSORES_ID
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_TIPO_SENSOR] G 
            ON F.DS_SENSORES_TIPO_SENSOR_ID = G.DS_TIPO_SENSOR_ID
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_FABRICANTES] H 
            ON H.DS_FABRICANTES_ID = G.DS_TIPO_SENSOR_FABRICANTE_ID
    WHERE 
        A.DS_STATUS = 1
        AND D.DS_STATUS = 1
        AND D.DS_DATA_DESATIVACAO IS NULL
        AND H.DS_FABRICANTES_DESCRIPTION NOT IN ('DEV Tecnologia', 'Histórico', 'Meraki - Cisco')
        AND D.DS_DATA_SOURCE_TM_ID NOT IN (7,8,13,14,16,20,21)
    GROUP BY 
        C.DS_COMPANY_DESCRIPTION,
        A.DS_SITE_DESCRIPTION,
        D.DS_DATA_SOURCE_DATA_INICIO
    ORDER BY 
        D.DS_DATA_SOURCE_DATA_INICIO DESC
    """

    cursor.execute(query)
    columns = [col[0] for col in cursor.description]

    data = [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

    cursor.close()
    return data

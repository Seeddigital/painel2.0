def get_sensores_desinstalados(conn):
    cursor = conn.cursor()

    query = """
    WITH SensoresPorChamado AS (
        SELECT 
            C.DS_COMPANY_DESCRIPTION AS CLIENTE,
            B.DS_SITE_DESCRIPTION AS LOJA,
            CAST(A.DS_CHAMADO_DT_AGENDAMENTO AS DATE) AS DATA,
            value AS SENSOR_ID
        FROM 
            [Seed_CFG_Analytics].[dbo].[DS_CHAMADOS] A
        INNER JOIN 
            [Seed_CFG_Analytics].[dbo].[DS_SITE] B 
            ON A.DS_CHAMADO_SITE_ID = B.DS_SITE_BS_ID
        INNER JOIN 
            [Seed_CFG_Analytics].[dbo].[DS_COMPANY] C 
            ON B.DS_SITE_COMPANY_ID_BS = C.DS_COMPANY_BS_ID
        CROSS APPLY 
            STRING_SPLIT(A.DS_CHAMADO_SENSOR_ID, ',')
        WHERE 
            A.DS_CHAMADO_TIPO = 'D'
            AND A.DS_CHAMADO_SENSOR_ID IS NOT NULL
    )
    SELECT 
        CLIENTE,
        LOJA,
        DATA,
        COUNT(*) AS TOTAL_SENSORES_DESINSTALADOS
    FROM 
        SensoresPorChamado
    GROUP BY 
        CLIENTE,
        LOJA,
        DATA
    ORDER BY 
        DATA DESC
    """

    cursor.execute(query)
    columns = [col[0] for col in cursor.description]

    data = [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

    cursor.close()
    return data

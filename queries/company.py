def get_company(conn):
    query = """
    SELECT
          [DS_COMPANY_DESCRIPTION]
        , [DS_COMPANY_EMPRESA_ID]
        , [DS_STATUS]
        , [DS_COMPANY_SENHA_INTEGRACAO]
    FROM [Seed_CFG_Analytics].[dbo].[DS_COMPANY]
    """
    
    cursor = conn.cursor()
    cursor.execute(query)

    # Captura dinamicamente os nomes das colunas
    columns = [column[0] for column in cursor.description]

    # Zipa cada linha com suas colunas → vira dicionário
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return results


def get_gaps_integracao_full(conn):
    query = """ 
    SELECT 
        [ID],
        [COMPANY],
        [SITE_ID],
        [SITE_NAME],
        [CNPJ],
        [DATA],
        [HORA],
        [FLUXO],
        [TICKET],
        [CHAVE]
    FROM [Seed_CFG_Analytics].[dbo].[DS_INTEGRACAO_GAP_FULL]
    ORDER BY [DATA] DESC, [HORA] ASC
    """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    
    results = []
    for row in cursor.fetchall():
        row = dict(zip(columns, row))

        if row["TICKET"] is None:
            row["STATUS_EMOJI"] = "🔴"
            row["STATUS_COLOR"] = "red"
        else:
            row["STATUS_EMOJI"] = "🟢"
            row["STATUS_COLOR"] = "green"

        results.append(row)
    return results

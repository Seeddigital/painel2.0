def get_gaps_integracao(conn):
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
    FROM [Seed_CFG_Analytics].[dbo].[DS_INTEGRACAO_GAP]
    ORDER BY [DATA] DESC, [HORA] ASC
    """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = []

    for row in cursor.fetchall():
        row = dict(zip(columns, row))

        # acrescentar transformações opcionais
        row["STATUS_COLOR"] = "red" if row["TICKET"] == "GAP" else "green"
        row["STATUS_EMOJI"] = "🔴" if row["TICKET"] == "GAP" else "🟢"

        results.append(row)

    return results

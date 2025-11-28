def get_integracao_ok(conn, data=None, site_id=None, offset=0, limit=5000):
    query = f""" 
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
    WHERE [TICKET] IS NOT NULL
    """

    params = []

    if data:
        query += " AND DATA = ?"
        params.append(data)

    if site_id:
        query += " AND SITE_ID = ?"
        params.append(site_id)

    query += " ORDER BY [DATA] DESC, [HORA] ASC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
    params.append(offset)
    params.append(limit)

    cursor = conn.cursor()
    cursor.execute(query, params)
    columns = [column[0] for column in cursor.description]

    results = []
    for row in cursor.fetchall():
        row = dict(zip(columns, row))
        # ENCODES OK status
        row["STATUS_EMOJI"] = "🟢"
        row["STATUS_COLOR"] = "green"
        results.append(row)

    return results

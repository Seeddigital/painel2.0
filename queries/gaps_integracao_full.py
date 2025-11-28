def get_gaps_integracao_full(conn):
    query = """
    SELECT 
        SITE_NAME,
        DATA,
        MAX(CASE WHEN HORA = 10 THEN CASE WHEN TICKET IS NULL THEN 0 ELSE 1 END END) AS H10,
        MAX(CASE WHEN HORA = 11 THEN CASE WHEN TICKET IS NULL THEN 0 ELSE 1 END END) AS H11,
        ...
        MAX(CASE WHEN HORA = 22 THEN CASE WHEN TICKET IS NULL THEN 0 ELSE 1 END END) AS H22
    FROM DS_INTEGRACAO_GAP_FULL
    WHERE DATA >= DATEADD(month, -2, GETDATE())
    GROUP BY SITE_NAME, DATA
    ORDER BY DATA DESC, SITE_NAME ASC
    """
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

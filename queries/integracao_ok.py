def get_integracao_ok(conn):
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
    WHERE [TICKET] IS NOT NULL
    ORDER BY [DATA] DESC, [HORA] ASC
    """
    ...

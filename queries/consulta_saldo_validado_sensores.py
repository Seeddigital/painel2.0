def get_saldo_validado_sensores(conn):
    cursor = conn.cursor()

    query = """
    SELECT  
        [id],
        [mes],
        [saldo_acumulado],
        [ativacao],
        [desativacao],
        [acumulado_ativacao],
        [desativacao_acumulada],
        [dt_carga]
    FROM 
        [Seed_db_API_Seed].[dbo].[Sensores_Saldo_Validado]
    ORDER BY 
        mes DESC
    """

    cursor.execute(query)
    columns = [col[0] for col in cursor.description]

    data = [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

    cursor.close()
    return data

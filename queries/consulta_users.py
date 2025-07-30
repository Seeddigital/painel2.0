def get_dados_users(conn):
    print("[DEBUG] Iniciando consulta de users")
    try:
        query = """



SELECT  [DS_USUARIOS_ID]
      ,[DS_USUARIOS_NOME]
      ,[DS_USUARIOS_EMAIL]
      ,[DS_USUARIOS_SENHA]
      ,[DS_STATUS]
      ,[DS_USUARIOS_PERFIL]
  FROM [Seed_CFG_Analytics].[dbo].[DS_USUARIOS]
  where ds_status = 1



  """

        cursor = conn.cursor()
        print("[DEBUG] Executando query...")
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        print(f"[DEBUG] Total de registros retornados: {len(results)}")
        return results

    except Exception as e:
        print(f"[ERRO] Falha ao executar query de users: {e}")
        raise HTTPException(status_code=500, detail="Erro ao consultar users")

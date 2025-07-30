def get_dados_sensores(conn):
    print("[DEBUG] Iniciando consulta de sensores")
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



def get_dados_estoque_detalhes(conn):
    query = """ 

  SELECT 

C.DS_USUARIOS_NOME
 ,[DS_HISTORICO_ESTOQUE_DT_ALTERACAO]
,[DS_HISTORICO_ESTOQUE_OPERACAO]
  ,[DS_HISTORICO_ESTOQUE_QTD_ALTERADA]
  ,b.DS_ESTOQUE_FABRICANTE
,B.DS_ESTOQUE_MODELO
,B.DS_ESTOQUE_TIPO
      
    
     
      
      
  FROM [Seed_CFG_Analytics].[dbo].[DS_HISTORICO_ESTOQUE]A 
  INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_ESTOQUE]B ON A.DS_HISTORICO_ESTOQUE_EQUIPAMENTO_ID = B.DS_ESTOQUE_ID
  INNER JOIN [Seed_CFG_Analytics].[dbo].DS_USUARIOS C on A.DS_HISTORICO_ESTOQUE_USUARIO_ID = C.DS_USUARIOS_ID


 """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return results

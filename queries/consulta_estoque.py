

def get_dados_lojas(conn):
    query = """ 
SELECT 
      [DS_ESTOQUE_TIPO]
      ,[DS_ESTOQUE_FABRICANTE]
      ,[DS_ESTOQUE_MODELO]
      ,[DS_ESTOQUE_QUANTIDADE]

  FROM [Seed_CFG_Analytics].[dbo].[DS_ESTOQUE]


 """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return results

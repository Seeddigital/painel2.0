
def get_dados_lojas(conn):
    query = """ 
SELECT
   S.DS_SITE_BS_ID  AS id,
    S.DS_SITE_DESCRIPTION AS name,
    C.DS_COMPANY_BS_ID AS client_id,
    S.DS_SITE_END_UF AS state,
    S.DS_SITE_END_CIDADE AS city,DS_SITE_CNPJ,
    CASE 
        WHEN S.DS_SITE_TS_ID = 2 THEN 'shopping'
        WHEN S.DS_SITE_TS_ID = 1 THEN 'rua'
        ELSE 'outro'
    END AS type,
    S.DS_SITE_COORDENADOR AS contact_name,
    S.DS_SITE_COORDENADOR AS contact_phone,
    S.DS_SITE_COORDENADOR AS contact_email,
    CONCAT(S.DS_SITE_END_LOGRADOURO, ', ', S.DS_SITE_END_NUMERO, ' - ', S.DS_SITE_DESCRIPTION) AS address,

  (
        SELECT COUNT(ss.DS_DATA_SOURCE_PK) 
        FROM [Seed_CFG_Analytics].[dbo].[DS_DATA_SOURCE] ss
        WHERE ss.DS_DATA_DESATIVACAO IS NULL 
          AND ss.DS_STATUS = 1
          AND ss.DS_DATA_SOURCE_SITE_ID = S.DS_SITE_BS_ID
    ) AS pk_numbers,

(
  SELECT TOP 1 B.DS_TECNICO_NOME
  FROM [Seed_CFG_Analytics].[dbo].[DS_CHAMADOS] A 
  INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_TECNICOS] B 
      ON A.DS_CHAMADO_TECNICO_ID = B.DS_TECNICO_ID
  WHERE A.DS_CHAMADO_SITE_ID = S.DS_SITE_BS_ID 
    AND A.DS_CHAMADO_TIPO = 'I'
  ORDER BY A.DS_CHAMADO_DT_AGENDAMENTO DESC  -- ou A.DS_CHAMADO_ID DESC se preferir
) AS Tecnico_responsavel,

    CASE 
        WHEN S.DS_SITE_HORA_INICIO IS NOT NULL AND S.DS_SITE_HORA_FIM IS NOT NULL 
            THEN LEFT(CONVERT(VARCHAR, S.DS_SITE_HORA_INICIO, 108), 5) + ' - ' + LEFT(CONVERT(VARCHAR, S.DS_SITE_HORA_FIM, 108), 5)
        ELSE NULL
    END AS week_hours,

    CASE 
        WHEN S.DS_SITE_SAT_HORA_INI IS NOT NULL AND S.DS_SITE_SAT_HORA_FIM IS NOT NULL 
            THEN LEFT(CONVERT(VARCHAR, S.DS_SITE_SAT_HORA_INI, 108), 5) + ' - ' + LEFT(CONVERT(VARCHAR, S.DS_SITE_SAT_HORA_FIM, 108), 5)
        ELSE NULL
    END AS saturday_hours,

    CASE 
        WHEN S.DS_SITE_SUN_HORA_INI IS NOT NULL AND S.DS_SITE_SUN_HORA_FIM IS NOT NULL 
            THEN LEFT(CONVERT(VARCHAR, S.DS_SITE_SUN_HORA_INI, 108), 5) + ' - ' + LEFT(CONVERT(VARCHAR, S.DS_SITE_SUN_HORA_FIM, 108), 5)
        ELSE NULL
    END AS sunday_hours,

    FORMAT(S.DS_SITE_DATA_INSTALACAO, 'yyyy-MM-dd') AS installation_date,
    --S.DS_SITE_TECNICO AS technician_responsible,
    FORMAT(S.DS_SITE_DATA_INSTALACAO, 'yyyy-MM-dd') AS installation_date,

    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM [Seed_CFG_Analytics].[dbo].[DS_SITE_SENSORES] SS 
            WHERE SS.DS_SITE_SENSORES_SITE_ID = S.DS_SITE_BS_ID 
              AND SS.DS_STATUS = 1
        ) THEN 'ativa'
        ELSE 'inativa'
    END AS status,

    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM [Seed_CFG_Analytics].[dbo].[DS_CHAMADOS] CM
            WHERE CM.DS_CHAMADO_SITE_ID = S.DS_SITE_BS_ID
              AND CM.DS_CHAMADO_TIPO = 'M'
              AND CM.DS_CHAMADO_STATUS = 1
        ) THEN 1
        ELSE 0
    END AS has_open_maintenance_call,


    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM [Seed_CFG_Analytics].[dbo].[DS_SITE_SENSORES] SS 
            WHERE SS.DS_SITE_SENSORES_SITE_ID = S.DS_SITE_BS_ID 
              AND SS.DS_STATUS = 1
        ) THEN 'ativa'
        ELSE 'inativa'
    END AS status

FROM 
    [Seed_CFG_Analytics].[dbo].[DS_SITE] S
INNER JOIN 
    [Seed_CFG_Analytics].[dbo].[DS_COMPANY] C 
    ON S.DS_SITE_COMPANY_ID_BS = C.DS_COMPANY_BS_ID
WHERE 
    S.DS_SITE_BS_ID IS NOT NULL
    and s.DS_STATUS = 1


 """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return results

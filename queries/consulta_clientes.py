WITH LojasPotencial AS (
    SELECT * FROM (
        VALUES 
            ('Shopping SP Market', 1),
            ('Artex', 90),
            ('MMMartan', 170),
            ('Galeria Pagé', 2),
            ('Colombo', 300),
            ('Enox', 0),
            ('Subway', 1861),
            ('Rosa Chá', 0),
            ('Arezzo&Co', 1052),
            ('ViaVarejo', 1127),
            ('Decathlon', 50),
            ('Malwee', 200),
            ('Raia Drogasil', 3100),
            ('ESPM', 9),
            ('Positivo', 0),
            ('Mitsubishi', 190),
            ('Almeida Junior', 6),
            ('Centauro', 230),
            ('Probel', 256),
            ('Shopping Flamboyant', 1),
            ('Oppa', 9),
            ('Reserva', 160),
            ('Lojão do Brás', 20),
            ('Flavio''s', 24),
            ('Novo Mundo', 160),
            ('Mega Moda', 3),
            ('Coppel', 50),
            ('Rensz', 8),
            ('Carrefour', 1000),
            ('Óticas Carol', 1400),
            ('Starbucks', 190),
            ('Lojas Americanas', 1700),
            ('Camila Klein', 1),
            ('JHSF', 4),
            ('O Boticário', 3700),
            ('RiHappy', 293),
            ('Magazine Luiza', 1400)
    ) AS T(DS_COMPANY_DESCRIPTION, QTD_LOJAS_POTENCIAL)
),
DataInstalacao AS (
    SELECT
        DS_SITE_COMPANY_ID_BS,
        MIN(DS_SITE_DATA_INSTALACAO) AS DATA_INSTALACAO_PRIMEIRO_SITE
    FROM [Seed_CFG_Analytics].[dbo].[DS_SITE]
    WHERE DS_SITE_DATA_INSTALACAO IS NOT NULL
    GROUP BY DS_SITE_COMPANY_ID_BS
),
UltimaManutencao AS (
    SELECT 
        S.DS_SITE_COMPANY_ID_BS,
        MAX(C.DS_CHAMADO_DT_AGENDAMENTO) AS DATA_ULTIMA_MANUTENCAO
    FROM [Seed_CFG_Analytics].[dbo].[DS_CHAMADOS] C
    INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_SITE] S
        ON C.DS_CHAMADO_SITE_ID = S.DS_SITE_BS_ID
    WHERE C.DS_CHAMADO_TIPO = 'M'
      AND C.DS_CHAMADO_DT_AGENDAMENTO IS NOT NULL
    GROUP BY S.DS_SITE_COMPANY_ID_BS
),
QtdLojas AS (
    SELECT 
        S.DS_SITE_COMPANY_ID_BS,
        COUNT(DISTINCT S.DS_SITE_BS_ID) AS QTD_LOJAS
    FROM [Seed_CFG_Analytics].[dbo].[DS_SITE] S
    INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_SITE_SENSORES] SS
        ON S.DS_SITE_BS_ID = SS.DS_SITE_SENSORES_SITE_ID
    WHERE SS.DS_STATUS = 1
    GROUP BY S.DS_SITE_COMPANY_ID_BS

)

SELECT DISTINCT 
    A.[DS_COMPANY_BS_ID],
    A.[DS_COMPANY_DESCRIPTION],
    C.DS_SEGMENTO_DESCRIPTION,

    -- Status Ativo/Inativo por existência de sensor ativo
    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM [Seed_CFG_Analytics].[dbo].[DS_SITE] S
            INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_SITE_SENSORES] SS
                ON S.DS_SITE_BS_ID = SS.DS_SITE_SENSORES_SITE_ID
            WHERE S.DS_SITE_COMPANY_ID_BS = A.DS_COMPANY_BS_ID
              AND SS.DS_STATUS = 1
        ) THEN 'Ativo'
        ELSE 'Inativo'
    END AS STATUS_CLIENTE,

    -- Quantidade de sensores ATIVOS
    (
        SELECT COUNT(DISTINCT ss.DS_SITE_SENSORES_ID)
    FROM [Seed_CFG_Analytics].[dbo].[DS_SITE] S
    INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_SITE_SENSORES] SS
        ON S.DS_SITE_BS_ID = SS.DS_SITE_SENSORES_SITE_ID
    INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_DATA_SOURCE] D
        ON S.DS_SITE_BS_ID = D.DS_DATA_SOURCE_SITE_ID
    WHERE S.DS_SITE_COMPANY_ID_BS = A.DS_COMPANY_BS_ID
      AND S.DS_STATUS = 1
      AND SS.DS_STATUS = 1
      AND D.DS_STATUS = 1
      AND D.DS_DATA_DESATIVACAO IS NULL
) AS QTD_SENSORES_ATIVOS,
  

    -- Quantidade de sensores INATIVOS
    (
        SELECT COUNT(DISTINCT SS.DS_SITE_SENSORES_SENSOR_ID)
        FROM [Seed_CFG_Analytics].[dbo].[DS_SITE] S
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_SITE_SENSORES] SS
            ON S.DS_SITE_BS_ID = SS.DS_SITE_SENSORES_SITE_ID
        INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_DATA_SOURCE] DS
            ON SS.DS_SITE_SENSORES_SENSOR_ID = DS.DS_DATA_SOURCE_SENSOR_ID
        WHERE S.DS_SITE_COMPANY_ID_BS = A.DS_COMPANY_BS_ID
          AND DS.DS_DATA_DESATIVACAO IS NOT NULL
          AND SS.DS_STATUS = 1
          AND DS.DS_STATUS = 1
          AND DS.DS_DATA_SOURCE_TM_ID = 1
    ) AS QTD_SENSORES_INATIVOS,

    LP.QTD_LOJAS_POTENCIAL,
    QL.QTD_LOJAS,
    DI.DATA_INSTALACAO_PRIMEIRO_SITE,
    UM.DATA_ULTIMA_MANUTENCAO

FROM [Seed_CFG_Analytics].[dbo].[DS_COMPANY] A
INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_NUCLEO] B 
    ON A.DS_COMPANY_BS_ID = B.DS_NUCLEO_COMPANY_ID_BS
INNER JOIN [Seed_CFG_Analytics].[dbo].[DS_SEGMENTO] C 
    ON B.DS_NUCLEO_SEGMENTO_ID = C.DS_SEGMENTO_ID
LEFT JOIN LojasPotencial LP
    ON LP.DS_COMPANY_DESCRIPTION = A.DS_COMPANY_DESCRIPTION
LEFT JOIN DataInstalacao DI
    ON DI.DS_SITE_COMPANY_ID_BS = A.DS_COMPANY_BS_ID
LEFT JOIN UltimaManutencao UM
    ON UM.DS_SITE_COMPANY_ID_BS = A.DS_COMPANY_BS_ID
LEFT JOIN QtdLojas QL
    ON QL.DS_SITE_COMPANY_ID_BS = A.DS_COMPANY_BS_ID

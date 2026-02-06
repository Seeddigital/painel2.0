from typing import Any, Dict, List


def _rows_to_dicts(cursor, rows) -> List[Dict[str, Any]]:
    cols = [col[0] for col in cursor.description]
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({cols[i]: r[i] for i in range(len(cols))})
    return out


def get_indice_nacional_headline(conn, mes_ano: str) -> List[Dict[str, Any]]:
    sql = """
    SELECT
        T.MES_ANO,
        T.INDICE_NACIONAL_B100,
        T.DISTANCIA_BASE_100,
        T.VARIACAO_PONTOS_MOM,
        T.FLUXO_ATUAL_TOTAL,
        T.FLUXO_ANTERIOR_TOTAL
    FROM (
        SELECT
            MES_ANO,
            AVG(INDICEB100_NACIONAL) AS INDICE_NACIONAL_B100,
            AVG(INDICEB100_NACIONAL) - 100 AS DISTANCIA_BASE_100,
            AVG(INDICEB100_NACIONAL) - LAG(AVG(INDICEB100_NACIONAL)) OVER (ORDER BY MES_ANO) AS VARIACAO_PONTOS_MOM,
            SUM(FLUXO_ATUAL) AS FLUXO_ATUAL_TOTAL,
            SUM(FLUXO_ANTERIOR) AS FLUXO_ANTERIOR_TOTAL
        FROM [Seed_db_INDICE_SD].[dbo].[INDICE_SEEDDIGITAL]
        GROUP BY MES_ANO
    ) T
    WHERE T.MES_ANO = ?;
    """
    cur = conn.cursor()
    cur.execute(sql, (mes_ano,))
    rows = cur.fetchall()
    return _rows_to_dicts(cur, rows)


def get_indice_nacional_serie(conn, from_mes_ano: str, to_mes_ano: str) -> List[Dict[str, Any]]:
    sql = """
    SELECT
        MES_ANO,
        AVG(INDICEB100_NACIONAL) AS INDICE_NACIONAL_B100
    FROM [Seed_db_INDICE_SD].[dbo].[INDICE_SEEDDIGITAL]
    WHERE MES_ANO >= ?
      AND MES_ANO <= ?
    GROUP BY MES_ANO
    ORDER BY MES_ANO;
    """
    cur = conn.cursor()
    cur.execute(sql, (from_mes_ano, to_mes_ano))
    rows = cur.fetchall()
    return _rows_to_dicts(cur, rows)


def get_indice_nacional_drivers(conn, mes_ano: str) -> List[Dict[str, Any]]:
    sql = """
    WITH BASE AS (
        SELECT
            MES_ANO,
            REGIAO,
            TIPO,
            SEGMENTO,
            INDICEB100_NACIONAL,
            INDICE_B100_REGIONAL,
            INDICE_B100_ESTADUAL,
            INDICE_NACIONAL,
            PESO_NACIONAL,
            PESO_TIPO
        FROM [Seed_db_INDICE_SD].[dbo].[INDICE_SEEDDIGITAL]
        WHERE MES_ANO = ?
    ),
    REGIAO_AGG AS (
        SELECT
            MES_ANO,
            REGIAO AS CHAVE,
            AVG(INDICE_B100_REGIONAL) AS INDICE_B100,
            SUM(PESO_NACIONAL) AS PESO_TOTAL,
            AVG(INDICE_B100_REGIONAL) * SUM(PESO_NACIONAL) AS IMPACTO
        FROM BASE
        GROUP BY MES_ANO, REGIAO
    ),
    TIPO_AGG AS (
        SELECT
            MES_ANO,
            TIPO AS CHAVE,
            AVG(INDICE_NACIONAL) AS INDICE,
            SUM(PESO_TIPO) AS PESO_TOTAL,
            AVG(INDICE_NACIONAL) * SUM(PESO_TIPO) AS IMPACTO
        FROM BASE
        GROUP BY MES_ANO, TIPO
    ),
    SEGMENTO_AGG AS (
        SELECT
            MES_ANO,
            SEGMENTO AS CHAVE,
            AVG(INDICE_NACIONAL) AS INDICE,
            SUM(PESO_NACIONAL) AS PESO_TOTAL,
            AVG(INDICE_NACIONAL) * SUM(PESO_NACIONAL) AS IMPACTO
        FROM BASE
        GROUP BY MES_ANO, SEGMENTO
    ),
    DISPERSAO AS (
        SELECT
            MES_ANO,
            MAX(INDICE_B100_REGIONAL) AS MELHOR_REGIAO_B100,
            MIN(INDICE_B100_REGIONAL) AS PIOR_REGIAO_B100,
            MAX(INDICE_B100_REGIONAL) - MIN(INDICE_B100_REGIONAL) AS DISPERSAO_PONTOS
        FROM REGIAO_AGG
        GROUP BY MES_ANO
    )
    SELECT
        'REGIAO' AS BLOCO,
        R.MES_ANO,
        R.CHAVE AS DIMENSAO,
        CAST(NULL AS NVARCHAR(50)) AS SUBDIMENSAO,
        CAST(R.INDICE_B100 AS DECIMAL(18,4)) AS INDICE_B100,
        CAST(NULL AS DECIMAL(18,4)) AS INDICE,
        CAST(R.PESO_TOTAL AS DECIMAL(18,6)) AS PESO_TOTAL,
        CAST(R.IMPACTO AS DECIMAL(18,6)) AS IMPACTO,
        CAST(NULL AS DECIMAL(18,4)) AS MELHOR_REF,
        CAST(NULL AS DECIMAL(18,4)) AS PIOR_REF,
        CAST(NULL AS DECIMAL(18,4)) AS DISPERSAO_PONTOS
    FROM REGIAO_AGG R
    UNION ALL
    SELECT
        'TIPO' AS BLOCO,
        T.MES_ANO,
        T.CHAVE AS DIMENSAO,
        CAST(NULL AS NVARCHAR(50)) AS SUBDIMENSAO,
        CAST(NULL AS DECIMAL(18,4)) AS INDICE_B100,
        CAST(T.INDICE AS DECIMAL(18,4)) AS INDICE,
        CAST(T.PESO_TOTAL AS DECIMAL(18,6)) AS PESO_TOTAL,
        CAST(T.IMPACTO AS DECIMAL(18,6)) AS IMPACTO,
        CAST(NULL AS DECIMAL(18,4)) AS MELHOR_REF,
        CAST(NULL AS DECIMAL(18,4)) AS PIOR_REF,
        CAST(NULL AS DECIMAL(18,4)) AS DISPERSAO_PONTOS
    FROM TIPO_AGG T
    UNION ALL
    SELECT
        'SEGMENTO' AS BLOCO,
        S.MES_ANO,
        S.CHAVE AS DIMENSAO,
        CAST(NULL AS NVARCHAR(50)) AS SUBDIMENSAO,
        CAST(NULL AS DECIMAL(18,4)) AS INDICE_B100,
        CAST(S.INDICE AS DECIMAL(18,4)) AS INDICE,
        CAST(S.PESO_TOTAL AS DECIMAL(18,6)) AS PESO_TOTAL,
        CAST(S.IMPACTO AS DECIMAL(18,6)) AS IMPACTO,
        CAST(NULL AS DECIMAL(18,4)) AS MELHOR_REF,
        CAST(NULL AS DECIMAL(18,4)) AS PIOR_REF,
        CAST(NULL AS DECIMAL(18,4)) AS DISPERSAO_PONTOS
    FROM SEGMENTO_AGG S
    UNION ALL
    SELECT
        'DISPERSAO' AS BLOCO,
        D.MES_ANO,
        CAST('REGIAO' AS NVARCHAR(50)) AS DIMENSAO,
        CAST(NULL AS NVARCHAR(50)) AS SUBDIMENSAO,
        CAST(NULL AS DECIMAL(18,4)) AS INDICE_B100,
        CAST(NULL AS DECIMAL(18,4)) AS INDICE,
        CAST(NULL AS DECIMAL(18,6)) AS PESO_TOTAL,
        CAST(NULL AS DECIMAL(18,6)) AS IMPACTO,
        CAST(D.MELHOR_REGIAO_B100 AS DECIMAL(18,4)) AS MELHOR_REF,
        CAST(D.PIOR_REGIAO_B100 AS DECIMAL(18,4)) AS PIOR_REF,
        CAST(D.DISPERSAO_PONTOS AS DECIMAL(18,4)) AS DISPERSAO_PONTOS
    FROM DISPERSAO D;
    """
    cur = conn.cursor()
    cur.execute(sql, (mes_ano,))
    rows = cur.fetchall()
    return _rows_to_dicts(cur, rows)

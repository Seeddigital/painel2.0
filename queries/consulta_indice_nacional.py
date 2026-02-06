from typing import Any, Dict, List
from datetime import datetime, date


def _rows_to_dicts(cursor, rows) -> List[Dict[str, Any]]:
    cols = [col[0] for col in cursor.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]


def _yyyymm_to_first_day(yyyymm: str) -> date:
    """
    Recebe 'YYYY-MM' e devolve date(YYYY, MM, 1).
    """
    try:
        dt = datetime.strptime(yyyymm, "%Y-%m")
        return date(dt.year, dt.month, 1)
    except Exception:
        raise ValueError(f"mes_ano inválido: '{yyyymm}'. Use formato YYYY-MM (ex: 2025-01).")


def get_indice_nacional_headline(conn, mes_ano: str) -> List[Dict[str, Any]]:
    mes_date = _yyyymm_to_first_day(mes_ano)

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
    cur.execute(sql, (mes_date,))
    return _rows_to_dicts(cur, cur.fetchall())


def get_indice_nacional_serie(conn, from_mes_ano: str, to_mes_ano: str) -> List[Dict[str, Any]]:
    from_date = _yyyymm_to_first_day(from_mes_ano)
    to_date = _yyyymm_to_first_day(to_mes_ano)

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
    cur.execute(sql, (from_date, to_date))
    return _rows_to_dicts(cur, cur.fetchall())


def get_indice_nacional_drivers(conn, mes_ano: str) -> List[Dict[str, Any]]:
    mes_date = _yyyymm_to_first_day(mes_ano)

    sql = """
    WITH BASE AS (
        SELECT
            MES_ANO,
            REGIAO,
            TIPO,
            SEGMENTO,
            INDICEB100_NACIONAL,
            PESO_NACIONAL,
            PESO_TIPO
        FROM [Seed_db_INDICE_SD].[dbo].[INDICE_SEEDDIGITAL]
        WHERE MES_ANO = ?
    ),

    REGIAO_AGG AS (
        SELECT
            MES_ANO,
            REGIAO AS DIMENSAO,
            AVG(INDICEB100_NACIONAL) AS INDICE_B100,
            SUM(PESO_NACIONAL) AS PESO_TOTAL,
            AVG(INDICEB100_NACIONAL) * SUM(PESO_NACIONAL) AS IMPACTO
        FROM BASE
        GROUP BY MES_ANO, REGIAO
    ),

    TIPO_AGG AS (
        SELECT
            MES_ANO,
            TIPO AS DIMENSAO,
            AVG(INDICEB100_NACIONAL) AS INDICE,
            SUM(PESO_TIPO) AS PESO_TOTAL,
            AVG(INDICEB100_NACIONAL) * SUM(PESO_TIPO) AS IMPACTO
        FROM BASE
        GROUP BY MES_ANO, TIPO
    ),

    SEGMENTO_AGG AS (
        SELECT
            MES_ANO,
            SEGMENTO AS DIMENSAO,
            AVG(INDICEB100_NACIONAL) AS INDICE,
            SUM(PESO_NACIONAL) AS PESO_TOTAL,
            AVG(INDICEB100_NACIONAL) * SUM(PESO_NACIONAL) AS IMPACTO
        FROM BASE
        GROUP BY MES_ANO, SEGMENTO
    ),

    DISPERSAO AS (
        SELECT
            MES_ANO,
            MAX(INDICE_B100) AS MELHOR_REF,
            MIN(INDICE_B100) AS PIOR_REF,
            MAX(INDICE_B100) - MIN(INDICE_B100) AS DISPERSAO_PONTOS
        FROM REGIAO_AGG
        GROUP BY MES_ANO
    )

    SELECT
        'REGIAO' AS BLOCO,
        MES_ANO,
        DIMENSAO,
        NULL AS SUBDIMENSAO,
        INDICE_B100,
        NULL AS INDICE,
        PESO_TOTAL,
        IMPACTO,
        NULL AS MELHOR_REF,
        NULL AS PIOR_REF,
        NULL AS DISPERSAO_PONTOS
    FROM REGIAO_AGG

    UNION ALL

    SELECT
        'TIPO',
        MES_ANO,
        DIMENSAO,
        NULL,
        NULL,
        INDICE,
        PESO_TOTAL,
        IMPACTO,
        NULL,
        NULL,
        NULL
    FROM TIPO_AGG

    UNION ALL

    SELECT
        'SEGMENTO',
        MES_ANO,
        DIMENSAO,
        NULL,
        NULL,
        INDICE,
        PESO_TOTAL,
        IMPACTO,
        NULL,
        NULL,
        NULL
    FROM SEGMENTO_AGG

    UNION ALL

    SELECT
        'DISPERSAO',
        MES_ANO,
        'REGIAO',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        MELHOR_REF,
        PIOR_REF,
        DISPERSAO_PONTOS
    FROM DISPERSAO;
    """

    cur = conn.cursor()
    cur.execute(sql, (mes_date,))
    return _rows_to_dicts(cur, cur.fetchall())

from typing import Any, Dict, List, Optional
from datetime import datetime, date


def _rows_to_dicts(cursor, rows) -> List[Dict[str, Any]]:
    cols = [col[0] for col in cursor.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date


# -----------------------------
# Helpers
# -----------------------------
def _rows_to_dicts(cursor, rows) -> List[Dict[str, Any]]:
    cols = [col[0] for col in cursor.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]


def _yyyymm_to_first_day(yyyymm: str) -> date:
    try:
        dt = datetime.strptime(yyyymm, "%Y-%m")
        return date(dt.year, dt.month, 1)
    except Exception:
        raise ValueError(f"mes_ano inválido: '{yyyymm}'. Use formato YYYY-MM (ex: 2025-01).")


def _date_to_yyyymm(d: Optional[date]) -> Optional[str]:
    if not d:
        return None
    return d.strftime("%Y-%m")


def _get_max_mes_ano(conn) -> date:
    cur = conn.cursor()
    cur.execute("SELECT MAX(MES_ANO) AS MAX_MES_ANO FROM [Seed_db_INDICE_SD].[dbo].[INDICE_SEEDDIGITAL];")
    row = cur.fetchone()
    if not row or not row[0]:
        raise ValueError("Tabela INDICE_SEEDDIGITAL está vazia (MAX(MES_ANO) retornou NULL).")
    return row[0]


def _meta_payload(
    mes_referencia: Optional[str],
    ultima_atualizacao: Optional[str],
) -> Dict[str, Any]:
    return {
        "mes_referencia": mes_referencia,  # "YYYY-MM"
        "ultima_atualizacao": ultima_atualizacao,  # "YYYY-MM"
        "escala": "pontos base 100",
        "fonte": "Seed",
        "observacao_metodologia": "Índice agregado por média do indicador nacional (convertido para Base 100). Drivers refletem contribuição MoM ponderada.",
    }


# =========================================================
# 1) HEADLINE
# - Padronizado Base 100 (multiplica por 100)
# - Retorna metadados
# - mês de retorno no formato "YYYY-MM"
# =========================================================
def get_indice_nacional_headline(conn, mes_ano: str) -> Dict[str, Any]:
    mes_date = _yyyymm_to_first_day(mes_ano)
    max_mes = _get_max_mes_ano(conn)

    sql = """
    WITH AGG AS (
        SELECT
            MES_ANO,
            AVG(INDICEB100_NACIONAL) * 100.0 AS INDICE_NACIONAL_B100,
            (AVG(INDICEB100_NACIONAL) * 100.0) - 100.0 AS DISTANCIA_BASE_100,
            (AVG(INDICEB100_NACIONAL) * 100.0)
              - LAG(AVG(INDICEB100_NACIONAL) * 100.0) OVER (ORDER BY MES_ANO) AS VARIACAO_PONTOS_MOM,
            SUM(FLUXO_ATUAL) AS FLUXO_ATUAL_TOTAL,
            SUM(FLUXO_ANTERIOR) AS FLUXO_ANTERIOR_TOTAL
        FROM [Seed_db_INDICE_SD].[dbo].[INDICE_SEEDDIGITAL]
        GROUP BY MES_ANO
    )
    SELECT
        MES_ANO,
        CAST(INDICE_NACIONAL_B100 AS DECIMAL(18,4)) AS INDICE_NACIONAL_B100,
        CAST(DISTANCIA_BASE_100 AS DECIMAL(18,4)) AS DISTANCIA_BASE_100,
        CAST(VARIACAO_PONTOS_MOM AS DECIMAL(18,4)) AS VARIACAO_PONTOS_MOM,
        CAST(FLUXO_ATUAL_TOTAL AS DECIMAL(18,0)) AS FLUXO_ATUAL_TOTAL,
        CAST(FLUXO_ANTERIOR_TOTAL AS DECIMAL(18,0)) AS FLUXO_ANTERIOR_TOTAL
    FROM AGG
    WHERE MES_ANO = ?;
    """

    cur = conn.cursor()
    cur.execute(sql, (mes_date,))
    rows = cur.fetchall()
    data = _rows_to_dicts(cur, rows)

    # normaliza MES_ANO no formato YYYY-MM
    for r in data:
        r["MES_ANO"] = _date_to_yyyymm(r.get("MES_ANO"))

    return {
        "meta": _meta_payload(
            mes_referencia=mes_ano,
            ultima_atualizacao=_date_to_yyyymm(max_mes),
        ),
        "data": data,
    }


# =========================================================
# 2) SÉRIE
# - Sempre intervalo: from/to OU ultimos
# - /serie?from=2023-01&to=2025-01
# - /serie?ultimos=24  (to default = MAX(MES_ANO))
# - Base 100
# - Retorna metadados
# =========================================================
def get_indice_nacional_serie(
    conn,
    from_: Optional[str] = None,
    to: Optional[str] = None,
    ultimos: Optional[int] = None,
) -> Dict[str, Any]:
    max_mes = _get_max_mes_ano(conn)

    if ultimos is not None:
        if ultimos <= 0:
            raise ValueError("ultimos deve ser um inteiro > 0.")
        to_date = _yyyymm_to_first_day(to) if to else max_mes
        # start = to_date - (ultimos-1) meses
        cur = conn.cursor()
        cur.execute("SELECT DATEADD(MONTH, ?, ?) AS FROM_DATE;", (-(ultimos - 1), to_date))
        from_date = cur.fetchone()[0]
    else:
        if not from_ or not to:
            raise ValueError("Informe from=YYYY-MM e to=YYYY-MM, OU ultimos=<int>.")
        from_date = _yyyymm_to_first_day(from_)
        to_date = _yyyymm_to_first_day(to)

    sql = """
    SELECT
        MES_ANO,
        CAST(AVG(INDICEB100_NACIONAL) * 100.0 AS DECIMAL(18,4)) AS INDICE_NACIONAL_B100
    FROM [Seed_db_INDICE_SD].[dbo].[INDICE_SEEDDIGITAL]
    WHERE MES_ANO >= ?
      AND MES_ANO <= ?
    GROUP BY MES_ANO
    ORDER BY MES_ANO;
    """

    cur2 = conn.cursor()
    cur2.execute(sql, (from_date, to_date))
    rows = cur2.fetchall()
    data = _rows_to_dicts(cur2, rows)

    for r in data:
        r["MES_ANO"] = _date_to_yyyymm(r.get("MES_ANO"))

    return {
        "meta": _meta_payload(
            mes_referencia=_date_to_yyyymm(to_date),
            ultima_atualizacao=_date_to_yyyymm(max_mes),
        )
        | {
            "from": _date_to_yyyymm(from_date),
            "to": _date_to_yyyymm(to_date),
            "ultimos": ultimos,
        },
        "data": data,
    }


# =========================================================
# 3) DRIVERS
# - Contribuição MoM por dimensão:
#   contribuicao = (indice_atual - indice_anterior) * peso_total
# - Base 100
# - Inclui top 2 positivos + top 1 negativo por bloco
# - Retorna metadados e dados ordenados
# =========================================================
def get_indice_nacional_drivers(
    conn,
    mes_ano: str,
    top_pos: int = 2,
    top_neg: int = 1,
) -> Dict[str, Any]:
    mes_date = _yyyymm_to_first_day(mes_ano)
    max_mes = _get_max_mes_ano(conn)

    # mês anterior
    cur = conn.cursor()
    cur.execute("SELECT DATEADD(MONTH, -1, ?) AS PREV_MES;", (mes_date,))
    prev_mes = cur.fetchone()[0]

    sql = """
    WITH BASE AS (
        SELECT
            MES_ANO,
            REGIAO,
            TIPO,
            SEGMENTO,
            (INDICEB100_NACIONAL * 100.0) AS INDICE_B100,
            PESO_NACIONAL,
            PESO_TIPO
        FROM [Seed_db_INDICE_SD].[dbo].[INDICE_SEEDDIGITAL]
        WHERE MES_ANO IN (?, ?)
    ),

    -- REGIAO
    REGIAO_M AS (
        SELECT
            MES_ANO,
            REGIAO AS DIMENSAO,
            AVG(INDICE_B100) AS INDICE_B100,
            SUM(PESO_NACIONAL) AS PESO_TOTAL
        FROM BASE
        GROUP BY MES_ANO, REGIAO
    ),
    REGIAO_JOIN AS (
        SELECT
            A.DIMENSAO,
            A.INDICE_B100 AS INDICE_ATUAL,
            B.INDICE_B100 AS INDICE_ANTERIOR,
            A.PESO_TOTAL AS PESO_TOTAL,
            (A.INDICE_B100 - B.INDICE_B100) AS DELTA_PONTOS,
            (A.INDICE_B100 - B.INDICE_B100) * A.PESO_TOTAL AS CONTRIBUICAO
        FROM REGIAO_M A
        LEFT JOIN REGIAO_M B
            ON B.DIMENSAO = A.DIMENSAO
           AND B.MES_ANO = ?
        WHERE A.MES_ANO = ?
    ),

    -- TIPO
    TIPO_M AS (
        SELECT
            MES_ANO,
            TIPO AS DIMENSAO,
            AVG(INDICE_B100) AS INDICE_B100,
            SUM(PESO_TIPO) AS PESO_TOTAL
        FROM BASE
        GROUP BY MES_ANO, TIPO
    ),
    TIPO_JOIN AS (
        SELECT
            A.DIMENSAO,
            A.INDICE_B100 AS INDICE_ATUAL,
            B.INDICE_B100 AS INDICE_ANTERIOR,
            A.PESO_TOTAL AS PESO_TOTAL,
            (A.INDICE_B100 - B.INDICE_B100) AS DELTA_PONTOS,
            (A.INDICE_B100 - B.INDICE_B100) * A.PESO_TOTAL AS CONTRIBUICAO
        FROM TIPO_M A
        LEFT JOIN TIPO_M B
            ON B.DIMENSAO = A.DIMENSAO
           AND B.MES_ANO = ?
        WHERE A.MES_ANO = ?
    ),

    -- SEGMENTO
    SEG_M AS (
        SELECT
            MES_ANO,
            SEGMENTO AS DIMENSAO,
            AVG(INDICE_B100) AS INDICE_B100,
            SUM(PESO_NACIONAL) AS PESO_TOTAL
        FROM BASE
        GROUP BY MES_ANO, SEGMENTO
    ),
    SEG_JOIN AS (
        SELECT
            A.DIMENSAO,
            A.INDICE_B100 AS INDICE_ATUAL,
            B.INDICE_B100 AS INDICE_ANTERIOR,
            A.PESO_TOTAL AS PESO_TOTAL,
            (A.INDICE_B100 - B.INDICE_B100) AS DELTA_PONTOS,
            (A.INDICE_B100 - B.INDICE_B100) * A.PESO_TOTAL AS CONTRIBUICAO
        FROM SEG_M A
        LEFT JOIN SEG_M B
            ON B.DIMENSAO = A.DIMENSAO
           AND B.MES_ANO = ?
        WHERE A.MES_ANO = ?
    ),

    DISP AS (
        SELECT
            MAX(INDICE_ATUAL) AS MELHOR_REF,
            MIN(INDICE_ATUAL) AS PIOR_REF,
            MAX(INDICE_ATUAL) - MIN(INDICE_ATUAL) AS DISPERSAO_PONTOS
        FROM REGIAO_JOIN
    )

    SELECT
        'REGIAO' AS BLOCO,
        ? AS MES_ANO,
        DIMENSAO,
        CAST(INDICE_ATUAL AS DECIMAL(18,4)) AS INDICE_ATUAL,
        CAST(INDICE_ANTERIOR AS DECIMAL(18,4)) AS INDICE_ANTERIOR,
        CAST(DELTA_PONTOS AS DECIMAL(18,4)) AS DELTA_PONTOS,
        CAST(PESO_TOTAL AS DECIMAL(18,6)) AS PESO_TOTAL,
        CAST(CONTRIBUICAO AS DECIMAL(18,6)) AS CONTRIBUICAO
    FROM REGIAO_JOIN

    UNION ALL

    SELECT
        'TIPO',
        ?,
        DIMENSAO,
        CAST(INDICE_ATUAL AS DECIMAL(18,4)),
        CAST(INDICE_ANTERIOR AS DECIMAL(18,4)),
        CAST(DELTA_PONTOS AS DECIMAL(18,4)),
        CAST(PESO_TOTAL AS DECIMAL(18,6)),
        CAST(CONTRIBUICAO AS DECIMAL(18,6))
    FROM TIPO_JOIN

    UNION ALL

    SELECT
        'SEGMENTO',
        ?,
        DIMENSAO,
        CAST(INDICE_ATUAL AS DECIMAL(18,4)),
        CAST(INDICE_ANTERIOR AS DECIMAL(18,4)),
        CAST(DELTA_PONTOS AS DECIMAL(18,4)),
        CAST(PESO_TOTAL AS DECIMAL(18,6)),
        CAST(CONTRIBUICAO AS DECIMAL(18,6))
    FROM SEG_JOIN

    UNION ALL

    SELECT
        'DISPERSAO',
        ?,
        'REGIAO' AS DIMENSAO,
        NULL,
        NULL,
        NULL,
        NULL,
        CAST(NULL AS DECIMAL(18,6))
    FROM DISP;
    """

    # parâmetros:
    # BASE mes_date, prev_mes
    # REGIAO_JOIN prev, cur
    # TIPO_JOIN prev, cur
    # SEG_JOIN prev, cur
    # MES_ANO repetido 4x no SELECT final
    params = (
        mes_date, prev_mes,          # BASE
        prev_mes, mes_date,          # REGIAO_JOIN
        prev_mes, mes_date,          # TIPO_JOIN
        prev_mes, mes_date,          # SEG_JOIN
        mes_date, mes_date, mes_date, mes_date  # SELECT final (MES_ANO)
    )

    cur2 = conn.cursor()
    cur2.execute(sql, params)
    rows = cur2.fetchall()
    raw = _rows_to_dicts(cur2, rows)

    # Normaliza MES_ANO para "YYYY-MM"
    for r in raw:
        r["MES_ANO"] = _date_to_yyyymm(r.get("MES_ANO"))

    # Separa blocos
    regiao = [r for r in raw if r.get("BLOCO") == "REGIAO"]
    tipo = [r for r in raw if r.get("BLOCO") == "TIPO"]
    segmento = [r for r in raw if r.get("BLOCO") == "SEGMENTO"]

    # Ordena por contribuição desc
    regiao_sorted = sorted(regiao, key=lambda x: (x.get("CONTRIBUICAO") is None, x.get("CONTRIBUICAO", 0)), reverse=True)
    tipo_sorted = sorted(tipo, key=lambda x: (x.get("CONTRIBUICAO") is None, x.get("CONTRIBUICAO", 0)), reverse=True)
    seg_sorted = sorted(segmento, key=lambda x: (x.get("CONTRIBUICAO") is None, x.get("CONTRIBUICAO", 0)), reverse=True)

    def _topcuts(arr: List[Dict[str, Any]]) -> Dict[str, Any]:
        pos = [x for x in arr if (x.get("CONTRIBUICAO") or 0) > 0]
        neg = [x for x in arr if (x.get("CONTRIBUICAO") or 0) < 0]
        pos_sorted = sorted(pos, key=lambda x: x.get("CONTRIBUICAO", 0), reverse=True)
        neg_sorted = sorted(neg, key=lambda x: x.get("CONTRIBUICAO", 0))
        return {
            "top_pos": pos_sorted[:top_pos],
            "top_neg": neg_sorted[:top_neg],
        }

    # Dispersão (calculada no SQL mas devolvemos como meta simples usando regiao)
    melhor = max((x.get("INDICE_ATUAL") for x in regiao if x.get("INDICE_ATUAL") is not None), default=None)
    pior = min((x.get("INDICE_ATUAL") for x in regiao if x.get("INDICE_ATUAL") is not None), default=None)
    dispersao_pontos = (float(melhor) - float(pior)) if (melhor is not None and pior is not None) else None

    return {
        "meta": _meta_payload(
            mes_referencia=mes_ano,
            ultima_atualizacao=_date_to_yyyymm(max_mes),
        )
        | {
            "mes_anterior": _date_to_yyyymm(prev_mes),
            "top_pos": top_pos,
            "top_neg": top_neg,
        },
        "data": {
            "regiao": regiao_sorted,
            "tipo": tipo_sorted,
            "segmento": seg_sorted,
            "dispersao": {
                "melhor_regiao_b100": melhor,
                "pior_regiao_b100": pior,
                "dispersao_pontos": dispersao_pontos,
            },
        },
        "top": {
            "regiao": _topcuts(regiao_sorted),
            "tipo": _topcuts(tipo_sorted),
            "segmento": _topcuts(seg_sorted),
        },
    }

def _yyyymm_to_first_day(yyyymm: str) -> date:
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


def get_indice_nacional_serie(
    conn,
    mes_ano: Optional[str] = None,
    from_mes_ano: Optional[str] = None,
    to_mes_ano: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Regras:
    - Se vier mes_ano => retorna somente aquele mês.
    - Senão, exige from_mes_ano e to_mes_ano => retorna a série no intervalo.
    """
    if mes_ano:
        from_date = _yyyymm_to_first_day(mes_ano)
        to_date = from_date
    else:
        if not from_mes_ano or not to_mes_ano:
            raise ValueError("Informe mes_ano=YYYY-MM OU from_mes_ano=YYYY-MM e to_mes_ano=YYYY-MM.")
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

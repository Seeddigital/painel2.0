from typing import Any, Dict, List, Optional
from datetime import datetime, date


# -----------------------------
# Helpers
# -----------------------------
def _rows_to_dicts(cursor, rows) -> List[Dict[str, Any]]:
    cols = [col[0] for col in cursor.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]


def _parse_mes_param(value: str) -> date:
    """
    Aceita:
      - 'YYYY-MM'
      - 'YYYY-MM-DD'
    Retorna sempre date(YYYY, MM, 1)
    """
    if not value:
        raise ValueError("Data vazia.")

    v = str(value).strip()

    # tenta YYYY-MM
    try:
        dt = datetime.strptime(v, "%Y-%m")
        return date(dt.year, dt.month, 1)
    except Exception:
        pass

    # tenta YYYY-MM-DD
    try:
        dt = datetime.strptime(v, "%Y-%m-%d")
        return date(dt.year, dt.month, 1)
    except Exception:
        pass

    raise ValueError(f"Data inválida: '{value}'. Use YYYY-MM (ex: 2025-01) ou YYYY-MM-DD.")


def _ensure_date_first_day(v: Any) -> date:
    """
    Garante que MES_ANO vire date(YYYY,MM,1) mesmo se vier:
      - date
      - datetime
      - str: 'YYYY-MM' ou 'YYYY-MM-DD'
    """
    if v is None:
        raise ValueError("Valor de data é NULL.")

    if isinstance(v, date) and not isinstance(v, datetime):
        return date(v.year, v.month, 1)

    if isinstance(v, datetime):
        return date(v.year, v.month, 1)

    # string (ou qualquer outro tipo) -> parse
    return _parse_mes_param(str(v))


def _date_to_yyyymm(v: Any) -> Optional[str]:
    """
    Retorna "YYYY-MM" aceitando date/datetime/str.
    """
    if v is None:
        return None

    d = _ensure_date_first_day(v)
    return d.strftime("%Y-%m")


def _get_max_mes_ano(conn) -> date:
    cur = conn.cursor()
    cur.execute("SELECT MAX(MES_ANO) AS MAX_MES_ANO FROM [Seed_db_INDICE_SD].[dbo].[INDICE_SEEDDIGITAL];")
    row = cur.fetchone()
    if not row or row[0] is None:
        raise ValueError("Tabela INDICE_SEEDDIGITAL está vazia (MAX(MES_ANO) retornou NULL).")

    # pode vir date/datetime/str -> normaliza
    return _ensure_date_first_day(row[0])


def _meta_payload(conn, mes_referencia: Optional[str]) -> Dict[str, Any]:
    max_mes = _get_max_mes_ano(conn)
    return {
        "mes_referencia": mes_referencia,  # "YYYY-MM"
        "ultima_atualizacao": _date_to_yyyymm(max_mes),
        "escala": "pontos base 100",
        "fonte": "Seed",
        "observacao_metodologia": "Índice agregado por média do indicador nacional (convertido para Base 100). Drivers refletem contribuição MoM ponderada.",
    }


# =========================================================
# HEADLINE (Base 100)
# =========================================================
def get_indice_nacional_headline(conn, mes_ano: str) -> Dict[str, Any]:
    mes_date = _parse_mes_param(mes_ano)

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
    data = _rows_to_dicts(cur, cur.fetchall())

    for r in data:
        r["MES_ANO"] = _date_to_yyyymm(r.get("MES_ANO"))

    return {
        "meta": _meta_payload(conn, _date_to_yyyymm(mes_date)),
        "data": data,
    }


# =========================================================
# SERIE (intervalo OU ultimos) - Base 100
# from/to via main.py (alias="from"/"to")
# =========================================================
def get_indice_nacional_serie(
    conn,
    from_mes_ano: Optional[str] = None,
    to_mes_ano: Optional[str] = None,
    ultimos: Optional[int] = None,
) -> Dict[str, Any]:
    max_mes = _get_max_mes_ano(conn)

    if ultimos is not None:
        if ultimos <= 0:
            raise ValueError("ultimos deve ser um inteiro > 0.")

        to_date = _parse_mes_param(to_mes_ano) if to_mes_ano else max_mes

        cur = conn.cursor()
        cur.execute("SELECT DATEADD(MONTH, ?, ?) AS FROM_DATE;", (-(ultimos - 1), to_date))
        from_date = cur.fetchone()[0]
        from_date = _ensure_date_first_day(from_date)

    else:
        if not from_mes_ano or not to_mes_ano:
            raise ValueError("Informe from=YYYY-MM e to=YYYY-MM, OU ultimos=<int>.")

        from_date = _parse_mes_param(from_mes_ano)
        to_date = _parse_mes_param(to_mes_ano)

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
    data = _rows_to_dicts(cur2, cur2.fetchall())

    for r in data:
        r["MES_ANO"] = _date_to_yyyymm(r.get("MES_ANO"))

    return {
        "meta": _meta_payload(conn, _date_to_yyyymm(to_date))
        | {
            "from": _date_to_yyyymm(from_date),
            "to": _date_to_yyyymm(to_date),
            "ultimos": ultimos,
        },
        "data": data,
    }


# =========================================================
# DRIVERS (MoM contribuição ponderada) - Base 100
# contribuicao = (indice_atual - indice_anterior) * peso_total
# + top_pos/top_neg
# =========================================================
def get_indice_nacional_drivers(
    conn,
    mes_ano: str,
    top_pos: int = 2,
    top_neg: int = 1,
) -> Dict[str, Any]:
    mes_date = _parse_mes_param(mes_ano)

    cur = conn.cursor()
    cur.execute("SELECT DATEADD(MONTH, -1, ?) AS PREV_MES;", (mes_date,))
    prev_mes = cur.fetchone()[0]
    prev_mes = _ensure_date_first_day(prev_mes)

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
    REG_M AS (
        SELECT MES_ANO, REGIAO AS DIMENSAO, AVG(INDICE_B100) AS INDICE_B100, SUM(PESO_NACIONAL) AS PESO_TOTAL
        FROM BASE GROUP BY MES_ANO, REGIAO
    ),
    REG_JOIN AS (
        SELECT
            A.DIMENSAO,
            A.INDICE_B100 AS INDICE_ATUAL,
            B.INDICE_B100 AS INDICE_ANTERIOR,
            A.PESO_TOTAL,
            (A.INDICE_B100 - B.INDICE_B100) AS DELTA_PONTOS,
            (A.INDICE_B100 - B.INDICE_B100) * A.PESO_TOTAL AS CONTRIBUICAO
        FROM REG_M A
        LEFT JOIN REG_M B ON B.DIMENSAO = A.DIMENSAO AND B.MES_ANO = ?
        WHERE A.MES_ANO = ?
    ),
    TIPO_M AS (
        SELECT MES_ANO, TIPO AS DIMENSAO, AVG(INDICE_B100) AS INDICE_B100, SUM(PESO_TIPO) AS PESO_TOTAL
        FROM BASE GROUP BY MES_ANO, TIPO
    ),
    TIPO_JOIN AS (
        SELECT
            A.DIMENSAO,
            A.INDICE_B100 AS INDICE_ATUAL,
            B.INDICE_B100 AS INDICE_ANTERIOR,
            A.PESO_TOTAL,
            (A.INDICE_B100 - B.INDICE_B100) AS DELTA_PONTOS,
            (A.INDICE_B100 - B.INDICE_B100) * A.PESO_TOTAL AS CONTRIBUICAO
        FROM TIPO_M A
        LEFT JOIN TIPO_M B ON B.DIMENSAO = A.DIMENSAO AND B.MES_ANO = ?
        WHERE A.MES_ANO = ?
    ),
    SEG_M AS (
        SELECT MES_ANO, SEGMENTO AS DIMENSAO, AVG(INDICE_B100) AS INDICE_B100, SUM(PESO_NACIONAL) AS PESO_TOTAL
        FROM BASE GROUP BY MES_ANO, SEGMENTO
    ),
    SEG_JOIN AS (
        SELECT
            A.DIMENSAO,
            A.INDICE_B100 AS INDICE_ATUAL,
            B.INDICE_B100 AS INDICE_ANTERIOR,
            A.PESO_TOTAL,
            (A.INDICE_B100 - B.INDICE_B100) AS DELTA_PONTOS,
            (A.INDICE_B100 - B.INDICE_B100) * A.PESO_TOTAL AS CONTRIBUICAO
        FROM SEG_M A
        LEFT JOIN SEG_M B ON B.DIMENSAO = A.DIMENSAO AND B.MES_ANO = ?
        WHERE A.MES_ANO = ?
    )
    SELECT 'REGIAO' AS BLOCO, ? AS MES_ANO, DIMENSAO,
           CAST(INDICE_ATUAL AS DECIMAL(18,4)) AS INDICE_ATUAL,
           CAST(INDICE_ANTERIOR AS DECIMAL(18,4)) AS INDICE_ANTERIOR,
           CAST(DELTA_PONTOS AS DECIMAL(18,4)) AS DELTA_PONTOS,
           CAST(PESO_TOTAL AS DECIMAL(18,6)) AS PESO_TOTAL,
           CAST(CONTRIBUICAO AS DECIMAL(18,6)) AS CONTRIBUICAO
    FROM REG_JOIN
    UNION ALL
    SELECT 'TIPO', ?, DIMENSAO,
           CAST(INDICE_ATUAL AS DECIMAL(18,4)),
           CAST(INDICE_ANTERIOR AS DECIMAL(18,4)),
           CAST(DELTA_PONTOS AS DECIMAL(18,4)),
           CAST(PESO_TOTAL AS DECIMAL(18,6)),
           CAST(CONTRIBUICAO AS DECIMAL(18,6))
    FROM TIPO_JOIN
    UNION ALL
    SELECT 'SEGMENTO', ?, DIMENSAO,
           CAST(INDICE_ATUAL AS DECIMAL(18,4)),
           CAST(INDICE_ANTERIOR AS DECIMAL(18,4)),
           CAST(DELTA_PONTOS AS DECIMAL(18,4)),
           CAST(PESO_TOTAL AS DECIMAL(18,6)),
           CAST(CONTRIBUICAO AS DECIMAL(18,6))
    FROM SEG_JOIN;
    """

    params = (
        mes_date, prev_mes,
        prev_mes, mes_date,
        prev_mes, mes_date,
        prev_mes, mes_date,
        mes_date, mes_date, mes_date
    )

    cur2 = conn.cursor()
    cur2.execute(sql, params)
    raw = _rows_to_dicts(cur2, cur2.fetchall())

    for r in raw:
        r["MES_ANO"] = _date_to_yyyymm(r.get("MES_ANO"))

    def _sort(arr):
        return sorted(arr, key=lambda x: (x.get("CONTRIBUICAO") is None, x.get("CONTRIBUICAO", 0)), reverse=True)

    regiao = _sort([x for x in raw if x.get("BLOCO") == "REGIAO"])
    tipo = _sort([x for x in raw if x.get("BLOCO") == "TIPO"])
    segmento = _sort([x for x in raw if x.get("BLOCO") == "SEGMENTO"])

    def _topcuts(arr):
        pos = sorted([x for x in arr if (x.get("CONTRIBUICAO") or 0) > 0], key=lambda x: x.get("CONTRIBUICAO", 0), reverse=True)
        neg = sorted([x for x in arr if (x.get("CONTRIBUICAO") or 0) < 0], key=lambda x: x.get("CONTRIBUICAO", 0))
        return {"top_pos": pos[:top_pos], "top_neg": neg[:top_neg]}

    melhor = max((x.get("INDICE_ATUAL") for x in regiao if x.get("INDICE_ATUAL") is not None), default=None)
    pior = min((x.get("INDICE_ATUAL") for x in regiao if x.get("INDICE_ATUAL") is not None), default=None)
    dispersao = (float(melhor) - float(pior)) if (melhor is not None and pior is not None) else None

    return {
        "meta": _meta_payload(conn, _date_to_yyyymm(mes_date))
        | {"mes_anterior": _date_to_yyyymm(prev_mes), "top_pos": top_pos, "top_neg": top_neg},
        "data": {
            "regiao": regiao,
            "tipo": tipo,
            "segmento": segmento,
            "dispersao": {
                "melhor_regiao_b100": melhor,
                "pior_regiao_b100": pior,
                "dispersao_pontos": dispersao,
            },
        },
        "top": {
            "regiao": _topcuts(regiao),
            "tipo": _topcuts(tipo),
            "segmento": _topcuts(segmento),
        },
    }

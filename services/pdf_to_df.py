#pdf_to_df.py
import pandas as pd

def rows_to_df(rows: list[dict]) -> pd.DataFrame:
    """
    Convierte todas las filas de tabla extraídas de un PDF en un DataFrame
    y agrega columnas derivadas (delta_usd, delta_pct).

    Parameters
    ----------
    rows : list[dict]
        Cada elemento es {**record, **num_record} construido en analyze_local().

    Returns
    -------
    pd.DataFrame
    """
    # ─── 1. construir tabla base ───────────────────────────────────────
    df = pd.DataFrame(rows)

    # ─── 2. normalizar nombres de columnas ─────────────────────────────
    COLMAP = {
        "Valor 30-abr":      "valor_prev_usd",
        "Valor Anterior USD":"valor_prev_usd",
        "Valor 31-may":      "valor_curr_usd",
        "Valor Actual USD":  "valor_curr_usd",
    }
    df.rename(columns=COLMAP, inplace=True)

    # ─── 3. convertir a float todas las numéricas ──────────────────────
    num_cols = [c for c in df.columns
                if c.startswith(("valor_", "rentabilidad", "delta"))]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    # ─── 4. agregar métricas derivadas ─────────────────────────────────
    if {"valor_prev_usd", "valor_curr_usd"}.issubset(df.columns):
        df["delta_usd"] = df["valor_curr_usd"] - df["valor_prev_usd"]
        df["delta_pct"] = df["delta_usd"] / df["valor_prev_usd"]

    return df
"""
Persistence helpers for raw market ingestion.

This module holds DB-facing batching logic so the sync orchestration stays
focused on control flow rather than storage details.
"""

from datetime import datetime
from typing import List, Tuple

import polars as pl


def prepare_batch_for_db(df: pl.DataFrame) -> Tuple[List[Tuple], List[Tuple]]:
    """
    Convert processed DataFrame into lists of tuples for DB insertion.
    Returns: (market_records, token_records)
    """
    if df.is_empty():
        return [], []

    # Handle potentially missing columns by adding them with defaults
    # This mimics the logic from the original db.py
    select_cols = []

    # ID
    if "id" in df.columns:
        select_cols.append(pl.col("id"))
    else:
        select_cols.append(pl.lit("").alias("id"))

    # Question
    if "question" in df.columns:
        select_cols.append(pl.col("question"))
    else:
        select_cols.append(pl.lit("").alias("question"))

    # Category
    if "category" in df.columns:
        select_cols.append(pl.col("category"))
    else:
        select_cols.append(pl.lit("").alias("category"))

    # Description
    if "description" in df.columns:
        select_cols.append(pl.col("description"))
    else:
        select_cols.append(pl.lit("").alias("description"))

    # Outcomes
    if "outcomes_str" in df.columns:
        select_cols.append(pl.col("outcomes_str"))
    else:
        select_cols.append(pl.lit("").alias("outcomes_str"))

    # Volume
    if "volumeNum" in df.columns:
        select_cols.append(pl.col("volumeNum").alias("volume"))
    elif "volume" in df.columns:
        select_cols.append(pl.col("volume"))
    else:
        select_cols.append(pl.lit(0.0).alias("volume"))

    # Active
    if "active" in df.columns:
        select_cols.append(pl.col("active"))
    else:
        select_cols.append(pl.lit(False).alias("active"))

    # Closed
    if "closed" in df.columns:
        select_cols.append(pl.col("closed"))
    else:
        select_cols.append(pl.lit(False).alias("closed"))

    # Created At
    if "created_at" in df.columns:
        select_cols.append(
            pl.col("created_at")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
            .alias("created_at_str")
        )
    else:
        select_cols.append(pl.lit("").alias("created_at_str"))

    # End Date
    if "end_date" in df.columns:
        select_cols.append(
            pl.col("end_date").dt.strftime("%Y-%m-%d %H:%M:%S").alias("end_date_str")
        )
    else:
        select_cols.append(pl.lit("").alias("end_date_str"))

    # Slug
    if "slug" in df.columns:
        select_cols.append(pl.col("slug"))
    else:
        select_cols.append(pl.lit(None).alias("slug"))

    # Event Slug
    if "event_slug" in df.columns:
        select_cols.append(pl.col("event_slug"))
    else:
        select_cols.append(pl.lit(None).alias("event_slug"))

    # Event Id
    if "event_id" in df.columns:
        select_cols.append(pl.col("event_id"))
    else:
        select_cols.append(pl.lit(None).alias("event_id"))

    # Tokens
    if "clobTokenIds_str" in df.columns:
        select_cols.append(pl.col("clobTokenIds_str"))
    else:
        select_cols.append(pl.lit("").alias("clobTokenIds_str"))

    # Select and iterate
    records_df = df.select(select_cols)
    scraped_at = datetime.now().isoformat()

    market_data = []
    token_data = []

    for row in records_df.rows():
        # Row: (id, question, ..., slug, event_slug, event_id, clobTokenIds)
        (
            m_id,
            q,
            cat,
            desc,
            out,
            vol,
            act,
            clo,
            cre,
            end_date,
            slug,
            event_slug,
            event_id,
            toks,
        ) = row

        market_data.append(
            (
                m_id,
                q,
                cat,
                desc,
                out,
                float(vol) if vol is not None else 0.0,
                bool(act) if act is not None else None,
                bool(clo) if clo is not None else None,
                cre,
                scraped_at,
                end_date,
                slug,
                event_slug,
                event_id,
            )
        )

        if toks and toks != "" and toks != "[]":
            token_data.append((m_id, toks))

    return market_data, token_data

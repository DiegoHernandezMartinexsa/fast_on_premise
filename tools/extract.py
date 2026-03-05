from typing import Generator

import polars as pl
from sqlglot import parse_one, exp
from sqlalchemy import text

from constants import AERUNID, AEDATTM, OPFLAG_VALUE
from tools.engine import create_sql_engine
from app_logging import get_logger


logger = get_logger(__name__)


def extract_data(
    query: str,
    page_size: int,
    init_date: str | None = None,
    end_date: str | None = None,
) -> Generator[pl.DataFrame, None, None]:
    params = {}
    if init_date and end_date:
        params["init_date"] = init_date
        params["end_date"] = end_date

    logger.info(f"Starting extraction with page_size={page_size}")

    engine = create_sql_engine()

    # Consecutivo global por corrida (entre chunks/parquets)
    recno_base = 0

    with engine.connect().execution_options(stream_results=True) as conn:
        result = conn.execute(text(query), params)

        while True:
            chunk = result.fetchmany(page_size)
            if not chunk:
                break

            records = [dict(row._mapping) for row in chunk]
            df = pl.DataFrame(records)

            n = df.height

            # AERECNO como string: 1..N global por corrida
            aerecno = pl.arange(recno_base + 1, recno_base + n + 1, eager=True).cast(pl.Utf8)
            recno_base += n

            # Auditoría (AERUNID/AEDATTM vienen estables desde constants.py)
            df = df.with_columns(
                pl.lit(AERUNID).cast(pl.Utf8).alias("AERUNID"),
                aerecno.alias("AERECNO"),
                # si AEDATTM viene con tz UTC (aware), Polars lo conserva; si no, igual queda consistente
                pl.lit(AEDATTM).cast(pl.Datetime).alias("AEDATTM"),
                pl.lit(OPFLAG_VALUE).cast(pl.Utf8).alias("OPFLAG"),  # siempre null
            )

            yield df

    logger.info("Extraction finished")


def prepare_query_for_extraction(query: str, extraction_type: str) -> str:
    """
    Prepares the query for extraction.
    If extraction_type is 'full', removes the WHERE clause.
    If extraction_type is 'cdc', returns the query as is.
    """
    if extraction_type.lower() == "cdc":
        # Replace first occurrence with :init_date and second with :end_date
        query = query.replace("$date", ":init_date", 1)
        query = query.replace("$date", ":end_date", 1)
        return query

    if extraction_type.lower() == "full":
        # Replace variable to avoid parsing issues
        parsed_query_str = query.replace("$bk_fecha", ":bk_fecha")

        try:
            expression = parse_one(parsed_query_str, read="tsql")
            select = expression.find(exp.Select)

            if select:
                # Remove WHERE clause
                select.set("where", None)

                final_query = expression.sql(dialect="tsql")
                # Restore variable
                final_query = final_query.replace(":bk_fecha", "$bk_fecha")

                logger.info("WHERE clause removed for FULL extraction")
                return final_query
            else:
                logger.warning("No SELECT statement found, returning original query")
                return query

        except Exception as e:
            logger.error(f"Error preparing query for FULL extraction: {e}")
            raise e

    return query
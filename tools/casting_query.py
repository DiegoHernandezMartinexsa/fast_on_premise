from sqlalchemy import text
from sqlglot import exp, parse_one

from tools.engine import create_sql_engine


def get_columns(table_name: str) -> list:
    engine = create_sql_engine()

    query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :table"
    params = {"table": table_name}

    with engine.connect() as connection:
        result = connection.execute(text(query), params)
        columns = [row[0] for row in result.fetchall()]

    return columns


def cast_query(query: str, table_name: str) -> str:
    columns = get_columns(table_name)

    parsed_query = query.replace("$bk_fecha", ":bk_fecha")

    expression = parse_one(parsed_query, read="tsql")
    select = expression.find(exp.Select)

    if select is None:
        return query

    projections = []
    for projection in select.expressions:
        if isinstance(projection, exp.Star):
            for column_name in columns:
                cast_expr = exp.Cast(
                    this=exp.Column(this=exp.to_identifier(column_name)),
                    to=exp.DataType.build("VARCHAR"),
                )
                alias = exp.Alias(this=cast_expr, alias=exp.to_identifier(column_name))
                projections.append(alias)
        else:
            projections.append(projection)

    select.set("expressions", projections)

    final_query = expression.sql(dialect="tsql")
    final_query = final_query.replace(":bk_fecha", "$bk_fecha")

    return final_query

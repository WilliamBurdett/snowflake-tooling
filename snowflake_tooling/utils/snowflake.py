import os
from typing import Dict, Any, List, Union

from snowflake.connector import SnowflakeConnection, connect


def get_snowflake_connection(role: str, **kwargs) -> SnowflakeConnection:
    user = kwargs.get("user", os.environ.get("SNOWFLAKE_USER"))
    password = kwargs.get("password", os.environ.get("SNOWFLAKE_PASSWORD"))
    account = kwargs.get("account", os.environ.get("SNOWFLAKE_ACCOUNT"))
    database = kwargs.get("database")

    params = {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
    }
    connection = connect(**params, role=role)
    return connection


def run_query(
    query: str,
    role: str,
    connection_params: Dict[str, str] = None,
    single_field_header=None,
) -> Union[List[Any], List[Dict[str, Any]]]:
    cursor = get_snowflake_connection(role, **connection_params).cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
    finally:
        cursor.close()
    if single_field_header is not None:
        return [row[single_field_header] for row in rows]
    return rows

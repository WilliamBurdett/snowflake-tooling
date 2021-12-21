from typing import List

from snowflake_tooling.utils.snowflake import run_query


def create_database_with_roles(
    database_name: str,
    roles_that_own: List[str] = None,
    roles_that_use: List[str] = None,
):
    objects = [
        ("USAGE", "SCHEMAS"),
        ("SELECT", "TABLES"),
        ("SELECT", "VIEWS"),
        ("USAGE", "FILE"),
        ("USAGE", "STAGES"),
        ("OPERATE", "TASKS"),
        ("OPERATE", "PIPES"),
        ("USAGE", "FUNCTIONS"),
        ("USAGE", "PROCEDURES"),
        ("USAGE", "SEQUENCES"),
    ]
    read_role = f"{database_name}_read_role"
    write_role = f"{database_name}_write_role"
    run_query(f"CREATE ROLE IF NOT EXISTS {write_role}", "USERADMIN")
    run_query(f"CREATE ROLE IF NOT EXISTS {read_role}", "USERADMIN")
    run_query(f"CREATE OR REPLACE DATABASE {database_name}", "SYSADMIN")
    run_query(
        f"GRANT OWNERSHIP ON DATABASE {database_name} to ROLE {write_role}",
        "SECURITYADMIN",
    )
    run_query(
        f"GRANT USAGE ON DATABASE {database_name} to ROLE {read_role}",
        "SECURITYADMIN",
    )
    run_query(
        f"DROP SCHEMA {database_name}.public",
        "SYSADMIN",
    )

    for object_permission, object_name in objects:
        run_query(
            f"""GRANT OWNERSHIP ON FUTURE {object_name}
    IN DATABASE {database_name} to ROLE {write_role}""",
            "SECURITYADMIN",
        )
        run_query(
            f"""GRANT {object_permission} ON FUTURE {object_name}
    IN DATABASE {database_name} to ROLE {read_role}""",
            "SECURITYADMIN",
        )

    if roles_that_own is None:
        roles_that_own = []
    if roles_that_use is None:
        roles_that_use = []
    for role in roles_that_own:
        run_query(f"GRANT ROLE {write_role} TO ROLE {role}", "SECURITYADMIN")
    for role in roles_that_use:
        run_query(f"GRANT ROLE {read_role} TO ROLE {role}", "SECURITYADMIN")

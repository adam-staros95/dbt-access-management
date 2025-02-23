import json
import os
import time
from datetime import datetime
from typing import List, Any, Dict

import yaml

from cli.access_mangement.access_management_config_parser import (
    parse_access_management_config,
    AccessManagementConfig,
)
from cli.access_mangement.access_management_rows_generator import (
    generate_access_management_rows,
    AccessManagementRow,
)
from cli.exceptions import AccessManagementConfigFileNotFoundException
from cli.exceptions import (
    DatabaseAccessManagementConfigNotExistsException,
)
from cli.model import ConfigureMacroProperties, ManifestNode


def _read_config_file(config_file_path: str) -> Dict[str, Any]:
    file_path = os.path.join(config_file_path)

    if not os.path.exists(file_path):
        raise AccessManagementConfigFileNotFoundException(file_path)

    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def _get_access_management_rows(
    manifest_nodes: List[ManifestNode],
    access_management_config: AccessManagementConfig,
    project_name: str,
    sql_engine: str,
    database_name: str = None,
) -> List[AccessManagementRow]:
    for database_access_config in access_management_config.databases_access_config:
        if database_access_config.database_name == database_name:
            return generate_access_management_rows(
                database_access_config, manifest_nodes, project_name, sql_engine
            )
    raise DatabaseAccessManagementConfigNotExistsException(database_name)


def _build_create_access_management_config_table_sql(
    rows: List[AccessManagementRow], table_name: str
) -> str:
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    create_table_sql = f"""
BEGIN;
CREATE SCHEMA IF NOT EXISTS access_management;
CREATE TABLE IF NOT EXISTS access_management.{table_name} (
        project_name TEXT,
        database_name TEXT,
        schema_name TEXT,
        model_name TEXT,
        materialization TEXT,
        identity_type TEXT,
        identity_name TEXT,
        grants SUPER,
        revokes SUPER,
        created_timestamp TIMESTAMP
    );
    """
    if rows:
        create_table_sql += f"""
        INSERT INTO access_management.{table_name}
        (project_name, database_name, schema_name, model_name, materialization, identity_type, identity_name, grants, revokes, created_timestamp)
        VALUES
        """

        values = []
        for row in rows:
            grants = json.dumps(list(row.grants)).replace("'", "''")
            revokes = json.dumps(list(row.revokes)).replace("'", "''")
            value = (
                f"('{row.project_name}', "
                f"'{row.database_name}', "
                f"'{row.schema_name}', "
                f"'{row.model_name}', "
                f"'{row.materialization}', "
                f"'{row.identity_type.value}', "
                f"'{row.identity_name}', "
                f"'{grants}', "
                f"'{revokes}', "
                f"TO_TIMESTAMP('{current_timestamp}', 'YYYY-MM-DD HH24:MI:SS'))"
            )
            values.append(value)

        create_table_sql += ",\n".join(values) + ";"

    create_table_sql += (
        f"DELETE FROM access_management.{table_name} "
        f"WHERE created_timestamp < TO_TIMESTAMP('{current_timestamp}', 'YYYY-MM-DD HH24:MI:SS');"
    )
    create_table_sql += "\nCOMMIT;"

    return create_table_sql


def get_configure_access_management_macro_properties(
    manifest_nodes: List[ManifestNode],
    config_file_path: str,
    sql_engine: str,
    database_name: str,
    project_name: str,
) -> ConfigureMacroProperties:
    config_file_data = _read_config_file(config_file_path)
    access_management_config = parse_access_management_config(config_file_data)
    access_management_rows = _get_access_management_rows(
        manifest_nodes,
        access_management_config,
        project_name,
        sql_engine,
        database_name,
    )

    temp_access_management_config_table_name = (
        f"temp_{project_name}_{int(time.time())}_access_management_config"
    )
    config_access_management_table_name = f"{project_name}_access_management_config"

    create_temp_access_management_config_table_query = (
        _build_create_access_management_config_table_sql(
            access_management_rows, temp_access_management_config_table_name
        )
    )
    create_access_management_config_table_query = (
        _build_create_access_management_config_table_sql(
            access_management_rows, config_access_management_table_name
        )
    )

    return ConfigureMacroProperties(
        temp_config_table_name=temp_access_management_config_table_name,
        config_table_name=config_access_management_table_name,
        create_temp_config_table_query=create_temp_access_management_config_table_query,
        create_config_table_query=create_access_management_config_table_query,
    )

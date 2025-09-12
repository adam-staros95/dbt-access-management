import json
import os
import time
from datetime import datetime
from typing import List, Any, Dict

import yaml

from cli.access_mangement.access_management_config_parser import (
    parse_access_management_config,
)
from cli.access_mangement.access_management_rows_generator import (
    generate_access_management_rows,
    AccessManagementRow,
)
from cli.constants import SQLEngine
from cli.exceptions import AccessManagementConfigFileNotFoundException
from cli.model import ConfigureMacroProperties, ManifestNode


def _read_config_file(config_file_path: str) -> Dict[str, Any]:
    file_path = os.path.join(config_file_path)

    if not os.path.exists(file_path):
        raise AccessManagementConfigFileNotFoundException(file_path)

    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def _build_create_access_management_config_table_sql_redshift(
    rows: List[AccessManagementRow],
    database_name: str,
    schema_name: str,
    table_name: str,
) -> str:
    table_location = f"{database_name}.{schema_name}"
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    create_table_sql = f"""
BEGIN;
CREATE SCHEMA IF NOT EXISTS {table_location};
CREATE TABLE IF NOT EXISTS {table_location}.{table_name} (
        project_name TEXT,
        database_name TEXT,
        schema_name TEXT,
        alias TEXT,
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
        INSERT INTO {table_location}.{table_name}
        (project_name, database_name, schema_name, alias, model_name, materialization,
        identity_type, identity_name, grants, revokes, created_timestamp)
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
                f"'{row.alias}', "
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
        f"DELETE FROM {table_location}.{table_name} "
        f"WHERE created_timestamp < TO_TIMESTAMP('{current_timestamp}', 'YYYY-MM-DD HH24:MI:SS');"
    )
    create_table_sql += "\nCOMMIT;"

    return create_table_sql


def _build_create_access_management_config_table_sql_databricks(
    rows: List[AccessManagementRow],
    database_name: str,
    schema_name: str,
    table_name: str,
) -> str:
    table_location = f"{database_name}.{schema_name}"
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    create_table_sql = f"""
BEGIN
CREATE SCHEMA IF NOT EXISTS {table_location};
CREATE OR REPLACE TABLE {table_location}.{table_name} (
        project_name STRING,
        database_name STRING,
        schema_name STRING,
        alias STRING,
        model_name STRING,
        materialization STRING,
        identity_type STRING,
        identity_name STRING,
        grants STRING,
        revokes STRING,
        created_timestamp TIMESTAMP
    );
    """
    if rows:
        create_table_sql += f"""
        INSERT INTO {table_location}.{table_name}
        (project_name, database_name, schema_name, alias, model_name, materialization,
        identity_type, identity_name, grants, revokes, created_timestamp)
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
                f"'{row.alias}', "
                f"'{row.model_name}', "
                f"'{row.materialization}', "
                f"'{row.identity_type.value}', "
                f"'{row.identity_name}', "
                f"'{grants}', "
                f"'{revokes}', "
                f"TO_TIMESTAMP('{current_timestamp}', 'yyyy-MM-dd HH:mm:ss'))"
            )
            values.append(value)

        create_table_sql += ",\n".join(values) + ";"
    create_table_sql += "\n END;"
    return create_table_sql


def get_configure_access_management_macro_properties(
    manifest_nodes: List[ManifestNode],
    config_file_path: str,
    sql_engine: str,
    project_name: str,
    database_name: str,
    schema_name: str,
) -> ConfigureMacroProperties:
    config_file_data = _read_config_file(config_file_path)
    access_management_config = parse_access_management_config(config_file_data)
    access_management_rows = generate_access_management_rows(
        access_management_config, manifest_nodes, project_name, sql_engine
    )

    temp_access_management_config_table_name = (
        f"temp_{project_name}_{int(time.time())}_access_management_config"
    )
    config_access_management_table_name = f"{project_name}_access_management_config"

    create_temp_access_management_config_table_query = (
        _build_create_access_management_config_table_sql_redshift(
            access_management_rows,
            database_name,
            schema_name,
            temp_access_management_config_table_name,
        )
        if sql_engine == SQLEngine.REDSHIFT
        else _build_create_access_management_config_table_sql_databricks(
            access_management_rows,
            database_name,
            schema_name,
            temp_access_management_config_table_name,
        )
    )
    create_access_management_config_table_query = (
        _build_create_access_management_config_table_sql_redshift(
            access_management_rows,
            database_name,
            schema_name,
            config_access_management_table_name,
        )
        if sql_engine == SQLEngine.REDSHIFT
        else _build_create_access_management_config_table_sql_databricks(
            access_management_rows,
            database_name,
            schema_name,
            config_access_management_table_name,
        )
    )

    return ConfigureMacroProperties(
        temp_config_table_name=temp_access_management_config_table_name,
        config_table_name=config_access_management_table_name,
        create_temp_config_table_query=create_temp_access_management_config_table_query,
        create_config_table_query=create_access_management_config_table_query,
        database_name=database_name,
        schema_name=schema_name,
    )

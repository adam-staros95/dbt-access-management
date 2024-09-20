import json
import time
from typing import List

from cli.data_masking.data_masking_config_file_parser import (
    parse_data_masking_config,
)
from cli.data_masking.data_masking_rows_generator import (
    generate_data_masking_rows,
    DataMaskingRow,
)
from cli.model import ConfigureMacroProperties, ManifestNode


def _build_create_data_masking_config_table_sql(
    rows: List[DataMaskingRow], table_name: str, project_name: str
) -> str:
    create_table_sql = f"""
BEGIN;
CREATE SCHEMA IF NOT EXISTS access_management;
DROP TABLE IF EXISTS access_management.{table_name};
CREATE TABLE access_management.{table_name} (
        project_name TEXT,
        database_name TEXT,
        schema_name TEXT,
        model_name TEXT,
        materialization TEXT,
        masking_config SUPER
    );
    """

    if rows:
        create_table_sql += f"""
        INSERT INTO access_management.{table_name}
        (project_name, database_name, schema_name, model_name, materialization, masking_config)
        VALUES
        """

        values = []
        for row in rows:
            masking_config = json.dumps(list(row.masking_config)).replace("'", "''")
            value = (
                f"('{project_name}', "
                f"'{row.database_name}', "
                f"'{row.schema_name}', "
                f"'{row.model_name}', "
                f"'{row.materialization}', "
                f"JSON_PARSE('{masking_config}'))"
            )
            values.append(value)

        create_table_sql += ",\n".join(values) + ";"
    create_table_sql += "\nCOMMIT;"

    return create_table_sql


def get_configure_data_masking_macro_properties(
    manifest_nodes: List[ManifestNode],
    config_file_path: str,
    project_name: str,
) -> ConfigureMacroProperties:
    data_masking_config = parse_data_masking_config(config_file_path)
    data_masking_rows = generate_data_masking_rows(data_masking_config, manifest_nodes)

    temp_data_masking_config_table_name = (
        f"temp_{project_name}_{int(time.time())}_data_masking_config"
    )
    config_data_masking_table_name = f"{project_name}_data_masking_config"

    create_temp_data_masking_config_table_query = (
        _build_create_data_masking_config_table_sql(
            data_masking_rows, temp_data_masking_config_table_name, project_name
        )
    )
    create_data_masking_config_table_query = (
        _build_create_data_masking_config_table_sql(
            data_masking_rows, config_data_masking_table_name, project_name
        )
    )

    return ConfigureMacroProperties(
        temp_config_table_name=temp_data_masking_config_table_name,
        config_table_name=config_data_masking_table_name,
        create_temp_config_table_query=create_temp_data_masking_config_table_query,
        create_config_table_query=create_data_masking_config_table_query,
    )

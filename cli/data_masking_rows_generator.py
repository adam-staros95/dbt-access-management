from typing import List, Dict
from pydantic import BaseModel
import os
from cli.data_masking_config_file_parser import (
    DataMaskingConfig,
)

from cli.constants import SUPPORTED_SQL_ENGINES
from cli.model import ManifestNode


class DataMaskingRow(BaseModel):
    database_name: str
    schema_name: str
    model_name: str
    materialization: str
    masking_config: List[Dict] = []


def generate_data_masking_rows(
    data_masking_config: DataMaskingConfig,
    manifest_nodes: List[ManifestNode],
    sql_engine: str,
) -> List[DataMaskingRow]:
    if sql_engine not in SUPPORTED_SQL_ENGINES:
        raise Exception(
            f"Currently supported sql engines are: {', '.join(SUPPORTED_SQL_ENGINES)}"
        )
    data_masking_rows = []

    for node in manifest_nodes:
        masking_config = []
        if os.name == "nt":
            node_for_system = node.path.replace("\\", "/")
        else:
            node_for_system = node
        for table_config in data_masking_config.table_masking_identities:
            if f"/{table_config.table_name}.sql" in node_for_system:
                for column in table_config.column_masking_identities:
                    masking_config.append(
                        {
                            "column_name": column.column_name,
                            "users_with_access": column.users_with_access,
                            "roles_with_access": column.roles_with_access,
                        }
                    )
                break

        access_management_row = DataMaskingRow(
            database_name=node.database_name,
            schema_name=node.schema_name,
            model_name=node.model_name,
            materialization=node.materialization,
            masking_config=masking_config,
        )
        data_masking_rows.append(access_management_row)
    return data_masking_rows

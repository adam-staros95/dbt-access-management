from typing import List, Dict

from pydantic import BaseModel

from cli.constants import SQLEngine
from cli.data_masking.data_masking_config_parser import (
    DataMaskingConfig,
)
from cli.exceptions import DatabricksColumnMaskingNotSupportedOnViewException
from cli.model import ManifestNode


class DataMaskingRow(BaseModel):
    project_name: str
    database_name: str
    schema_name: str
    alias: str
    model_name: str
    materialization: str
    masking_config: List[Dict] = []


def generate_data_masking_rows(
    data_masking_config: DataMaskingConfig,
    manifest_nodes: List[ManifestNode],
    project_name: str,
    sql_engine: str,
) -> List[DataMaskingRow]:
    data_masking_rows = []

    for node in manifest_nodes:
        masking_config = []
        for model_config in data_masking_config.model_masking_identities:
            if model_config.model_name == node.model_name:
                for column in model_config.column_masking_identities:
                    base_config = {
                        "column_name": column.column_name,
                        "users_with_access": column.users_with_access,
                    }
                    if sql_engine == SQLEngine.REDSHIFT:
                        base_config["roles_with_access"] = column.roles_with_access
                    else:
                        base_config["groups_with_access"] = column.groups_with_access

                    masking_config.append(base_config)
                break
        if (
            masking_config
            and sql_engine == SQLEngine.DATABRICKS
            and node.materialization.lower() == "view"
        ):
            raise DatabricksColumnMaskingNotSupportedOnViewException(
                model_name=node.model_name
            )
        data_masking_row = DataMaskingRow(
            project_name=project_name,
            database_name=node.database_name,
            schema_name=node.schema_name,
            alias=node.alias,
            model_name=node.model_name,
            materialization=node.materialization,
            masking_config=masking_config,
        )
        data_masking_rows.append(data_masking_row)
    return data_masking_rows

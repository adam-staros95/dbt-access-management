from typing import List, Dict

from pydantic import BaseModel

from cli.data_masking.data_masking_config_file_parser import (
    DataMaskingConfig,
)
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
) -> List[DataMaskingRow]:
    data_masking_rows = []

    for node in manifest_nodes:
        masking_config = []
        for model_config in data_masking_config.model_masking_identities:
            if model_config.model_name == node.model_name:
                for column in model_config.column_masking_identities:
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

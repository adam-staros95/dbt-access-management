import os
from typing import List, Any, Dict

import yaml
from pydantic import BaseModel


class ColumnMaskingConfig(BaseModel):
    column_name: str
    users_with_access: List[str]
    roles_with_access: List[str]


class ModelDataMaskingConfig(BaseModel):
    model_name: str
    column_masking_identities: List[ColumnMaskingConfig]


class DataMaskingConfig(BaseModel):
    model_masking_identities: List[ModelDataMaskingConfig]


def _read_config_file(config_file_path: str) -> Dict[str, Any]:
    file_path = os.path.join(config_file_path)

    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def _parse_columns_config(columns_config: List[Dict]) -> List[ColumnMaskingConfig]:
    columns_masking_config = []
    for column_config in columns_config:
        for column_name, masking_config in column_config.items():
            columns_masking_config.append(
                ColumnMaskingConfig(
                    column_name=column_name,
                    users_with_access=masking_config.get("users_with_access", []),
                    roles_with_access=masking_config.get("roles_with_access", []),
                )
            )
    return columns_masking_config


def parse_data_masking_config(config_file_path: str) -> DataMaskingConfig:
    data = _read_config_file(config_file_path)
    tables_config = data["configuration"]
    data_masking_config = []
    for table_config in tables_config:
        for table_name, columns in table_config.items():
            columns_masking_config = _parse_columns_config(columns["columns"])
            data_masking_config.append(
                ModelDataMaskingConfig(
                    model_name=table_name,
                    column_masking_identities=columns_masking_config,
                )
            )
    return DataMaskingConfig(model_masking_identities=data_masking_config)

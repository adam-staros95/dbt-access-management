from enum import Enum

from pydantic import BaseModel


class ModelType(str, Enum):
    MODEL = "model"
    SEED = "seed"
    SNAPSHOT = "snapshot"


class ManifestNode(BaseModel):
    database_name: str
    model_type: ModelType
    alias: str
    model_name: str
    schema_name: str
    materialization: str
    path: str


class ConfigureMacroProperties(BaseModel):
    temp_config_table_name: str
    config_table_name: str
    create_temp_config_table_query: str
    create_config_table_query: str
    database_name: str
    schema_name: str

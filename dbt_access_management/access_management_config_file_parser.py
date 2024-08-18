import os
from enum import Enum
from typing import List, Any, Dict, Tuple

import yaml
from pydantic import BaseModel


class EntityType(str, Enum):
    USER = "user"
    GROUP = "group"
    ROLE = "role"


class AccessLevel(str, Enum):
    READ = "read"
    WRITE = "write"
    READ_WRITE = "read_write"
    ALL = "all"


class AccessConfigEntity(BaseModel):
    entity_type: EntityType
    entity_name: str
    config_paths: List[Tuple[str, AccessLevel]]


class DataBaseAccessConfig(BaseModel):
    database_name: str
    access_config_entities: List[AccessConfigEntity]


class AccessManagementConfig(BaseModel):
    databases_access_config: List[DataBaseAccessConfig]


def _read_config_file(config_file_path: str) -> Dict[str, Any]:
    file_path = os.path.join(config_file_path)

    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def _extract_config_paths(
    config: Dict[str, Any], current_path: str
) -> List[Tuple[str, AccessLevel]]:
    config_paths = []
    for key, value in config.items():
        if key.startswith("+access_level"):
            access_level = AccessLevel(value)
            config_paths.append((current_path, access_level))
        else:
            new_path = current_path + key + "/"
            config_paths.extend(_extract_config_paths(value, new_path))
    return config_paths


def parse_access_management_config(config_file_path: str) -> AccessManagementConfig:
    data = _read_config_file(config_file_path)
    databases = data["databases"]
    databases_access_config = []

    for database_name, entities in databases.items():
        users_config = entities.get("users", {})
        roles_config = entities.get("roles", {})
        groups_config = entities.get("groups", {})

        users_entities = [
            AccessConfigEntity(
                entity_name=entity_name,
                config_paths=_extract_config_paths(config, "/"),
                entity_type=EntityType.USER,
            )
            for entity_name, config in users_config.items()
        ]

        roles_entities = [
            AccessConfigEntity(
                entity_name=entity_name,
                config_paths=_extract_config_paths(config, "/"),
                entity_type=EntityType.ROLE,
            )
            for entity_name, config in roles_config.items()
        ]

        groups_entities = [
            AccessConfigEntity(
                entity_name=entity_name,
                config_paths=_extract_config_paths(config, "/"),
                entity_type=EntityType.GROUP,
            )
            for entity_name, config in groups_config.items()
        ]

        databases_access_config.append(
            DataBaseAccessConfig(
                database_name=database_name,
                access_config_entities=users_entities
                + roles_entities
                + groups_entities,
            )
        )

    return AccessManagementConfig(databases_access_config=databases_access_config)

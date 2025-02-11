from enum import Enum
from typing import List, Any, Dict, Tuple

from pydantic import BaseModel


class IdentityType(str, Enum):
    USER = "user"
    GROUP = "group"
    ROLE = "role"


class AccessLevel(str, Enum):
    READ = "read"
    WRITE = "write"
    READ_WRITE = "read_write"
    ALL = "all"


class AccessConfigIdentity(BaseModel):
    identity_type: IdentityType
    identity_name: str
    config_paths: List[Tuple[str, AccessLevel]]


class DataBaseAccessConfig(BaseModel):
    database_name: str
    access_config_identities: List[AccessConfigIdentity]


class AccessManagementConfig(BaseModel):
    databases_access_config: List[DataBaseAccessConfig]


# Visible for tests
def extract_configs(
    config: Dict[str, Any], current_path: str = "/"
) -> List[Tuple[str, AccessLevel]]:
    config_paths = []
    for key, value in config.items():
        if key.startswith("+access_level"):
            access_level = AccessLevel(value)
            config_paths.append((current_path, access_level))
        else:
            new_path = current_path + key + "/"
            config_paths.extend(extract_configs(value, new_path))
    return config_paths


def parse_access_management_config(data: Dict[str, Any]) -> AccessManagementConfig:
    databases = data["databases"]
    databases_access_config = []

    for database_name, entities in databases.items():
        users_config = entities.get("users", {})
        roles_config = entities.get("roles", {})
        groups_config = entities.get("groups", {})

        users_entities = [
            AccessConfigIdentity(
                identity_name=identity_name,
                config_paths=extract_configs(config, "/"),
                identity_type=IdentityType.USER,
            )
            for identity_name, config in users_config.items()
        ]

        roles_entities = [
            AccessConfigIdentity(
                identity_name=identity_name,
                config_paths=extract_configs(config, "/"),
                identity_type=IdentityType.ROLE,
            )
            for identity_name, config in roles_config.items()
        ]

        groups_entities = [
            AccessConfigIdentity(
                identity_name=identity_name,
                config_paths=extract_configs(config, "/"),
                identity_type=IdentityType.GROUP,
            )
            for identity_name, config in groups_config.items()
        ]

        databases_access_config.append(
            DataBaseAccessConfig(
                database_name=database_name,
                access_config_identities=users_entities
                + roles_entities
                + groups_entities,
            )
        )

    return AccessManagementConfig(databases_access_config=databases_access_config)

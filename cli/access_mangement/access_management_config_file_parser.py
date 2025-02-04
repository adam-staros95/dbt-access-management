import os
from enum import Enum
from typing import List, Any, Dict, Tuple

import yaml
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
    config_paths: List[
        Tuple[str, AccessLevel]
    ]  # TODO: Rename to model_path_access_configs [ModelPathAccessConfig(model_path="/", access_level=AccessLevel.READ)],
    column_level_configs: Dict[str, Dict[str, AccessLevel]] = {}


class DataBaseAccessConfig(BaseModel):
    database_name: str
    access_config_identities: List[AccessConfigIdentity]


class AccessManagementConfig(BaseModel):
    databases_access_config: List[DataBaseAccessConfig]


def _read_config_file(config_file_path: str) -> Dict[str, Any]:
    file_path = os.path.join(config_file_path)

    with open(file_path, "r") as file:
        return yaml.safe_load(file)


# TODO: Add unit tests
# Visible for tests
def extract_configs(
        config: Dict[str, Any], current_path: str = "/"
) -> Tuple[List[Tuple[str, AccessLevel]], Dict[str, Dict[str, AccessLevel]]]:
    config_paths = []
    column_level_configs = {}

    for key, value in config.items():
        if key == "columns":
            table_name = current_path.rstrip("/")
            if table_name not in column_level_configs:
                column_level_configs[table_name] = {}

            for column in value:
                column_name = None
                column_access_level = None

                if isinstance(column, dict):
                    for col_key, col_value in column.items():
                        if col_key == "name":
                            column_name = col_value
                        elif col_key == "+access_level":
                            column_access_level = AccessLevel(col_value)

                    if column_name and column_access_level:
                        column_level_configs[table_name][
                            column_name
                        ] = column_access_level

        elif key.startswith("+access_level"):
            access_level = AccessLevel(value)
            config_paths.append((current_path, access_level))
        else:
            new_path = current_path + key + "/"
            sub_paths, sub_columns = extract_configs(value, new_path)
            config_paths.extend(sub_paths)
            for table, cols in sub_columns.items():
                if table not in column_level_configs:
                    column_level_configs[table] = {}
                column_level_configs[table].update(cols)

    return config_paths, column_level_configs


def parse_access_management_config(config_file_path: str) -> AccessManagementConfig:
    data = _read_config_file(config_file_path)
    databases = data["databases"]
    databases_access_config = []

    for database_name, identities in databases.items():
        users_config = identities.get("users", {})
        roles_config = identities.get("roles", {})
        groups_config = identities.get("groups", {})

        def create_access_config_identities(
                config: Dict[str, Any], identity_type: IdentityType
        ) -> List[AccessConfigIdentity]:
            return [
                AccessConfigIdentity(
                    identity_type=identity_type,
                    identity_name=identity_name,
                    config_paths=extract_configs(config)[0],
                    column_level_configs=extract_configs(config)[1],
                )
                for identity_name, config in config.items()
            ]

        users_identities = create_access_config_identities(
            users_config, IdentityType.USER
        )
        roles_identities = create_access_config_identities(
            roles_config, IdentityType.ROLE
        )
        groups_identities = create_access_config_identities(
            groups_config, IdentityType.GROUP
        )

        databases_access_config.append(
            DataBaseAccessConfig(
                database_name=database_name,
                access_config_identities=users_identities
                                         + roles_identities
                                         + groups_identities,
            )
        )

    return AccessManagementConfig(databases_access_config=databases_access_config)

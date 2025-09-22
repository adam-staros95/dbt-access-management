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


class AccessManagementConfig(BaseModel):
    access_config_identities: List[AccessConfigIdentity]


def _extract_configs(
    config: Dict[str, Any], current_path: str = "/"
) -> List[Tuple[str, AccessLevel]]:
    config_paths = []
    for key, value in config.items():
        if key.startswith("+access_level"):
            access_level = AccessLevel(value)
            config_paths.append((current_path, access_level))
        else:
            new_path = current_path + key + "/"
            config_paths.extend(_extract_configs(value, new_path))
    return config_paths


def parse_access_management_config(data: Dict[str, Any]) -> AccessManagementConfig:
    raw_config = data.get("configuration", {})
    config = raw_config if isinstance(raw_config, dict) else {}

    identity_map = {
        "users": IdentityType.USER,
        "roles": IdentityType.ROLE,
        "groups": IdentityType.GROUP,
    }

    access_entities: List[AccessConfigIdentity] = []

    for key, identity_type in identity_map.items():
        for identity_name, identity_config in config.get(key, {}).items():
            access_entities.append(
                AccessConfigIdentity(
                    identity_name=identity_name,
                    config_paths=_extract_configs(identity_config, "/"),
                    identity_type=identity_type,
                )
            )

    return AccessManagementConfig(access_config_identities=access_entities)

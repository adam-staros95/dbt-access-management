from typing import List, Set

from pydantic import BaseModel

from cli.access_management_config_file_parser import (
    IdentityType,
    DataBaseAccessConfig,
    AccessLevel,
    AccessConfigIdentity,
)
from cli.constants import SUPPORTED_SQL_ENGINES
from cli.model import ManifestNode, ModelType


class AccessManagementRow(BaseModel):
    project_name: str
    database_name: str
    schema_name: str
    model_name: str
    materialization: str
    identity_type: IdentityType
    identity_name: str
    grants: Set[str] = {}
    revokes: Set[str] = {}


def generate_access_management_rows(
    data_base_access_config: DataBaseAccessConfig,
    manifest_nodes: List[ManifestNode],
    project_name: str,
    sql_engine: str,
) -> List[AccessManagementRow]:
    if sql_engine not in SUPPORTED_SQL_ENGINES:
        raise Exception(
            f"Currently supported sql engines are: {', '.join(SUPPORTED_SQL_ENGINES)}"
        )
    access_management_rows = []

    for node in manifest_nodes:
        for identity in data_base_access_config.access_config_identities:
            grants_per_node = set()
            revokes_per_node = set()

            sorted_config_paths = sorted(
                identity.config_paths, key=lambda x: x[0].count("/"), reverse=True
            )

            for path, access_level in sorted_config_paths:
                if node.model_type == ModelType.MODEL:
                    if f"/{node.path.replace('.sql', '/')}".startswith(path):
                        grants_per_node = _get_grant_statements(
                            access_level, identity, node
                        )
                        revokes_per_node = _get_revoke_statements(
                            access_level, identity, node
                        )
                        break

                if node.model_type == ModelType.SEED:
                    if f"/{node.path.replace('.csv', '/')}".startswith(path):
                        grants_per_node = _get_grant_statements(
                            access_level, identity, node
                        )
                        revokes_per_node = _get_revoke_statements(
                            access_level, identity, node
                        )
                        break

            access_management_row = AccessManagementRow(
                project_name=project_name,
                database_name=node.database_name,
                schema_name=node.schema_name,
                model_name=node.model_name,
                materialization=node.materialization,
                identity_type=identity.identity_type,
                identity_name=identity.identity_name,
                grants=grants_per_node,
                revokes=revokes_per_node,
            )
            access_management_rows.append(access_management_row)

    return access_management_rows


def _get_identity_name_with_keyword_for_identity_type(
    identity: AccessConfigIdentity,
) -> str:
    if identity.identity_type == IdentityType.ROLE:
        return f'ROLE \\"{identity.identity_name}\\"'
    elif identity.identity_type == IdentityType.GROUP:
        return f'GROUP \\"{identity.identity_name}\\"'
    else:
        return f'\\"{identity.identity_name}\\"'


def _get_grant_statements(
    access_level: AccessLevel, entity: AccessConfigIdentity, node: ManifestNode
) -> Set[str]:
    grants = set()
    identity_name_with_keyword = _get_identity_name_with_keyword_for_identity_type(
        entity
    )

    grants.add(
        f"GRANT USAGE ON SCHEMA {node.schema_name} TO {identity_name_with_keyword};"
    )
    if access_level == AccessLevel.READ:
        grants.add(
            f"GRANT SELECT ON {node.schema_name}.{node.model_name} TO {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.WRITE:
        grants.add(
            f"GRANT INSERT ON {node.schema_name}.{node.model_name} TO {identity_name_with_keyword};"
        )
        grants.add(
            f"GRANT UPDATE ON {node.schema_name}.{node.model_name} TO {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.READ_WRITE:
        grants.add(
            f"GRANT SELECT ON {node.schema_name}.{node.model_name} TO {identity_name_with_keyword};"
        )
        grants.add(
            f"GRANT INSERT ON {node.schema_name}.{node.model_name} TO {identity_name_with_keyword};"
        )
        grants.add(
            f"GRANT UPDATE ON {node.schema_name}.{node.model_name} TO {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.ALL:
        grants.add(
            f"GRANT ALL ON {node.schema_name}.{node.model_name} TO {identity_name_with_keyword};"
        )

    return grants


def _get_revoke_statements(
    access_level: AccessLevel,
    entity: AccessConfigIdentity,
    node: ManifestNode,
) -> Set[str]:
    revokes = set()
    identity_name_with_keyword = _get_identity_name_with_keyword_for_identity_type(
        entity
    )

    if access_level == AccessLevel.READ:
        revokes.add(
            f"REVOKE SELECT ON {node.schema_name}.{node.model_name} FROM {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.WRITE:
        revokes.add(
            f"REVOKE INSERT ON {node.schema_name}.{node.model_name} FROM {identity_name_with_keyword};"
        )
        revokes.add(
            f"REVOKE UPDATE ON {node.schema_name}.{node.model_name} FROM {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.READ_WRITE:
        revokes.add(
            f"REVOKE SELECT ON {node.schema_name}.{node.model_name} FROM {identity_name_with_keyword};"
        )
        revokes.add(
            f"REVOKE INSERT ON {node.schema_name}.{node.model_name} FROM {identity_name_with_keyword};"
        )
        revokes.add(
            f"REVOKE UPDATE ON {node.schema_name}.{node.model_name} FROM {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.ALL:
        revokes.add(
            f"REVOKE ALL ON {node.schema_name}.{node.model_name} FROM {identity_name_with_keyword};"
        )

    return revokes

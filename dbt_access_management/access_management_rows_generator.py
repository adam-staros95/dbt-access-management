from typing import List, Set

from pydantic import BaseModel

from dbt_access_management.access_management_config_file_parser import (
    EntityType,
    DataBaseAccessConfig,
    AccessLevel,
    AccessConfigEntity,
)
from dbt_access_management.constants import SUPPORTED_SQL_ENGINES
from dbt_access_management.model import ManifestNode, ModelType


class AccessManagementRow(BaseModel):
    project_name: str
    database_name: str
    schema_name: str
    model_name: str
    materialization: str
    entity_type: EntityType
    entity_name: str
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
        for entity in data_base_access_config.access_config_entities:
            grants_per_node = set()
            revokes_per_node = set()

            sorted_config_paths = sorted(
                entity.config_paths, key=lambda x: x[0].count("/"), reverse=True
            )

            for path, access_level in sorted_config_paths:
                if node.model_type == ModelType.MODEL:
                    if f"/{node.path.replace('.sql', '/')}".startswith(path):
                        grants_per_node = _get_grant_statements(
                            access_level, entity, node
                        )
                        revokes_per_node = _get_revoke_statements(
                            access_level, entity, node
                        )
                        break

                if node.model_type == ModelType.SEED:
                    if f"/{node.path.replace('.csv', '/')}".startswith(path):
                        grants_per_node = _get_grant_statements(
                            access_level, entity, node
                        )
                        revokes_per_node = _get_revoke_statements(
                            access_level, entity, node
                        )
                        break

            access_management_row = AccessManagementRow(
                project_name=project_name,
                database_name=node.database_name,
                schema_name=node.schema_name,
                model_name=node.model_name,
                materialization=node.materialization,
                entity_type=entity.entity_type,
                entity_name=entity.entity_name,
                grants=grants_per_node,
                revokes=revokes_per_node,
            )
            access_management_rows.append(access_management_row)

    return access_management_rows


def _get_entity_name_with_keyword_for_entity_type(entity: AccessConfigEntity) -> str:
    return f"""{f'ROLE {entity.entity_name}' if entity.entity_type == EntityType.ROLE
    else f'GROUP {entity.entity_name}' if entity.entity_type == EntityType.GROUP
    else f'{entity.entity_name}'}"""


def _get_grant_statements(
    access_level: AccessLevel, entity: AccessConfigEntity, node: ManifestNode
) -> Set[str]:
    grants = set()
    entity_name_with_keyword = _get_entity_name_with_keyword_for_entity_type(entity)

    grants.add(
        f"GRANT USAGE ON SCHEMA {node.schema_name} TO {entity_name_with_keyword};"
    )
    if access_level == AccessLevel.READ:
        grants.add(
            f"GRANT SELECT ON {node.schema_name}.{node.model_name} TO {entity_name_with_keyword};"
        )
    if access_level == AccessLevel.WRITE:
        grants.add(
            f"GRANT INSERT ON {node.schema_name}.{node.model_name} TO {entity_name_with_keyword};"
        )
        grants.add(
            f"GRANT UPDATE ON {node.schema_name}.{node.model_name} TO {entity_name_with_keyword};"
        )
    if access_level == AccessLevel.READ_WRITE:
        grants.add(
            f"GRANT SELECT ON {node.schema_name}.{node.model_name} TO {entity_name_with_keyword};"
        )
        grants.add(
            f"GRANT INSERT ON {node.schema_name}.{node.model_name} TO {entity_name_with_keyword};"
        )
        grants.add(
            f"GRANT UPDATE ON {node.schema_name}.{node.model_name} TO {entity_name_with_keyword};"
        )
    if access_level == AccessLevel.ALL:
        grants.add(
            f"GRANT ALL ON {node.schema_name}.{node.model_name} TO {entity_name_with_keyword};"
        )

    return grants


def _get_revoke_statements(
    access_level: AccessLevel,
    entity: AccessConfigEntity,
    node: ManifestNode,
) -> Set[str]:
    revokes = set()
    entity_name_with_keyword = _get_entity_name_with_keyword_for_entity_type(entity)

    if access_level == AccessLevel.READ:
        revokes.add(
            f"REVOKE SELECT ON {node.schema_name}.{node.model_name} FROM {entity_name_with_keyword};"
        )
    if access_level == AccessLevel.WRITE:
        revokes.add(
            f"REVOKE INSERT ON {node.schema_name}.{node.model_name} FROM {entity_name_with_keyword};"
        )
        revokes.add(
            f"REVOKE UPDATE ON {node.schema_name}.{node.model_name} FROM {entity_name_with_keyword};"
        )
    if access_level == AccessLevel.READ_WRITE:
        revokes.add(
            f"REVOKE SELECT ON {node.schema_name}.{node.model_name} FROM {entity_name_with_keyword};"
        )
        revokes.add(
            f"REVOKE INSERT ON {node.schema_name}.{node.model_name} FROM {entity_name_with_keyword};"
        )
        revokes.add(
            f"REVOKE UPDATE ON {node.schema_name}.{node.model_name} FROM {entity_name_with_keyword};"
        )
    if access_level == AccessLevel.ALL:
        revokes.add(
            f"REVOKE ALL ON {node.schema_name}.{node.model_name} FROM {entity_name_with_keyword};"
        )

    return revokes

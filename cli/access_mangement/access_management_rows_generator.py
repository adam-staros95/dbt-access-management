from typing import List, Set

from pydantic import BaseModel

from cli.access_mangement.access_management_config_parser import (
    IdentityType,
    AccessLevel,
    AccessConfigIdentity,
    AccessManagementConfig,
)
from cli.constants import SUPPORTED_SQL_ENGINES, SQLEngine
from cli.exceptions import NotSupportedMaterializationException
from cli.model import ManifestNode, ModelType


class AccessManagementRow(BaseModel):
    project_name: str
    database_name: str
    schema_name: str
    alias: str
    model_name: str
    materialization: str
    identity_type: IdentityType
    identity_name: str
    grants: Set[str] = {}
    revokes: Set[str] = {}


def generate_access_management_rows(
    config: AccessManagementConfig,
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
        for identity in config.access_config_identities:
            grants_per_node = set()
            revokes_per_node = set()

            sorted_config_paths = sorted(
                identity.config_paths, key=lambda x: x[0].count("/"), reverse=True
            )

            for path, access_level in sorted_config_paths:
                if (
                    node.model_type == ModelType.MODEL
                    or node.model_type == ModelType.SNAPSHOT
                ):
                    if f"/{node.path.replace('.sql', '/')}".startswith(path):
                        grants_per_node = (
                            _get_grant_statements_redshift(access_level, identity, node)
                            if sql_engine == SQLEngine.REDSHIFT
                            else _get_grant_statements_databricks(
                                access_level,
                                identity,
                                node,
                            )
                        )
                        revokes_per_node = (
                            _get_revoke_statements_redshift(
                                access_level, identity, node
                            )
                            if sql_engine == SQLEngine.REDSHIFT
                            else _get_revoke_statements_databricks(
                                access_level,
                                identity,
                                node,
                            )
                        )
                        break

                if node.model_type == ModelType.SEED:
                    if f"/{node.path.replace('.csv', '/')}".startswith(path):
                        grants_per_node = (
                            _get_grant_statements_redshift(
                                access_level,
                                identity,
                                node,
                            )
                            if sql_engine == SQLEngine.REDSHIFT
                            else _get_grant_statements_databricks(
                                access_level,
                                identity,
                                node,
                            )
                        )
                        revokes_per_node = (
                            _get_revoke_statements_redshift(
                                access_level,
                                identity,
                                node,
                            )
                            if sql_engine == SQLEngine.REDSHIFT
                            else _get_revoke_statements_databricks(
                                access_level,
                                identity,
                                node,
                            )
                        )
                        break

            access_management_row = AccessManagementRow(
                project_name=project_name,
                database_name=node.database_name,
                schema_name=node.schema_name,
                alias=node.alias,
                model_name=node.model_name,
                materialization=node.materialization,
                identity_type=identity.identity_type,
                identity_name=identity.identity_name,
                grants=grants_per_node,
                revokes=revokes_per_node,
            )
            access_management_rows.append(access_management_row)

    return access_management_rows


def _get_identity_name_with_keyword_for_identity_type_redshift(
    identity: AccessConfigIdentity,
) -> str:
    if identity.identity_type == IdentityType.ROLE:
        return f'ROLE \\"{identity.identity_name}\\"'
    elif identity.identity_type == IdentityType.GROUP:
        return f'GROUP \\"{identity.identity_name}\\"'
    else:
        return f'\\"{identity.identity_name}\\"'


def _get_grant_statements_redshift(
    access_level: AccessLevel,
    identity: AccessConfigIdentity,
    node: ManifestNode,
) -> Set[str]:
    grants = set()
    identity_name_with_keyword = (
        _get_identity_name_with_keyword_for_identity_type_redshift(identity)
    )

    grants.add(
        f"GRANT USAGE ON SCHEMA {node.schema_name} TO {identity_name_with_keyword};"
    )
    if access_level == AccessLevel.READ:
        grants.add(
            f"GRANT SELECT ON {node.schema_name}.{node.alias} TO {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.WRITE:
        grants.add(
            f"GRANT INSERT ON {node.schema_name}.{node.alias} TO {identity_name_with_keyword};"
        )
        grants.add(
            f"GRANT UPDATE ON {node.schema_name}.{node.alias} TO {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.READ_WRITE:
        grants.add(
            f"GRANT SELECT ON {node.schema_name}.{node.alias} TO {identity_name_with_keyword};"
        )
        grants.add(
            f"GRANT INSERT ON {node.schema_name}.{node.alias} TO {identity_name_with_keyword};"
        )
        grants.add(
            f"GRANT UPDATE ON {node.schema_name}.{node.alias} TO {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.ALL:
        grants.add(
            f"GRANT ALL ON {node.schema_name}.{node.alias} TO {identity_name_with_keyword};"
        )

    return grants


def _get_revoke_statements_redshift(
    access_level: AccessLevel,
    identity: AccessConfigIdentity,
    node: ManifestNode,
) -> Set[str]:
    revokes = set()
    identity_name_with_keyword = (
        _get_identity_name_with_keyword_for_identity_type_redshift(identity)
    )

    if access_level == AccessLevel.READ:
        revokes.add(
            f"REVOKE SELECT ON {node.schema_name}.{node.alias} FROM {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.WRITE:
        revokes.add(
            f"REVOKE INSERT ON {node.schema_name}.{node.alias} FROM {identity_name_with_keyword};"
        )
        revokes.add(
            f"REVOKE UPDATE ON {node.schema_name}.{node.alias} FROM {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.READ_WRITE:
        revokes.add(
            f"REVOKE SELECT ON {node.schema_name}.{node.alias} FROM {identity_name_with_keyword};"
        )
        revokes.add(
            f"REVOKE INSERT ON {node.schema_name}.{node.alias} FROM {identity_name_with_keyword};"
        )
        revokes.add(
            f"REVOKE UPDATE ON {node.schema_name}.{node.alias} FROM {identity_name_with_keyword};"
        )
    if access_level == AccessLevel.ALL:
        revokes.add(
            f"REVOKE ALL ON {node.schema_name}.{node.alias} FROM {identity_name_with_keyword};"
        )

    return revokes


def _get_materialization_to_securable_object_type_databricks(
    materialization: str,
) -> str:
    materialization_to_securable_object_type_map = {
        "view": "VIEW",
        "materialized_view": "MATERIALIZED VIEW",
        "table": "TABLE",
        "streaming_table": "TABLE",
        "incremental": "TABLE",
    }
    securable_object_type = materialization_to_securable_object_type_map.get(
        materialization.lower(), None
    )
    if not securable_object_type:
        raise NotSupportedMaterializationException(
            provided_materialization=materialization,
            supported_materializations=list(
                materialization_to_securable_object_type_map.values()
            ),
        )
    return securable_object_type


def _get_grant_statements_databricks(
    access_level: AccessLevel,
    identity: AccessConfigIdentity,
    node: ManifestNode,
) -> Set[str]:
    grants = set()

    securable_object_type = _get_materialization_to_securable_object_type_databricks(
        node.materialization
    )

    grants.add(
        f"GRANT USE CATALOG ON CATALOG {node.database_name} TO `{identity.identity_name}`;"
    )
    grants.add(
        f"GRANT USE SCHEMA ON SCHEMA {node.database_name}.{node.schema_name} TO `{identity.identity_name}`;"
    )
    if access_level == AccessLevel.READ:
        grants.add(
            f"GRANT SELECT ON {securable_object_type} {node.database_name}.{node.schema_name}.{node.alias} "
            f"TO `{identity.identity_name}`;"
        )
    if access_level == AccessLevel.WRITE:
        grants.add(
            f"GRANT MODIFY ON {securable_object_type} {node.database_name}.{node.schema_name}.{node.alias} "
            f"TO `{identity.identity_name}`;"
        )
    if access_level == AccessLevel.READ_WRITE:
        grants.add(
            f"GRANT SELECT ON {securable_object_type} {node.database_name}.{node.schema_name}.{node.alias} "
            f"TO `{identity.identity_name}`;"
        )
        grants.add(
            f"GRANT MODIFY ON {securable_object_type} {node.database_name}.{node.schema_name}.{node.alias} "
            f"TO `{identity.identity_name}`;"
        )
    if access_level == AccessLevel.ALL:
        grants.add(
            f"GRANT ALL PRIVILEGES ON {securable_object_type} {node.database_name}.{node.schema_name}.{node.alias} "
            f"TO `{identity.identity_name}`;"
        )
    return grants


def _get_revoke_statements_databricks(
    access_level: AccessLevel,
    identity: AccessConfigIdentity,
    node: ManifestNode,
) -> Set[str]:
    revokes = set()

    securable_object_type = _get_materialization_to_securable_object_type_databricks(
        node.materialization
    )

    if access_level == AccessLevel.READ:
        revokes.add(
            f"REVOKE SELECT ON {securable_object_type} {node.database_name}.{node.schema_name}.{node.alias} "
            f"FROM `{identity.identity_name}`;"
        )
    if access_level == AccessLevel.WRITE:
        revokes.add(
            f"REVOKE MODIFY ON {securable_object_type} {node.database_name}.{node.schema_name}.{node.alias} "
            f"FROM `{identity.identity_name}`;"
        )
    if access_level == AccessLevel.READ_WRITE:
        revokes.add(
            f"REVOKE SELECT ON {securable_object_type} {node.database_name}.{node.schema_name}.{node.alias} "
            f"FROM `{identity.identity_name}`;"
        )
        revokes.add(
            f"REVOKE MODIFY ON {securable_object_type} {node.database_name}.{node.schema_name}.{node.alias} "
            f"FROM `{identity.identity_name}`;"
        )
    if access_level == AccessLevel.ALL:
        revokes.add(
            f"REVOKE ALL PRIVILEGES ON {securable_object_type} {node.database_name}.{node.schema_name}.{node.alias} "
            f"FROM `{identity.identity_name}`;"
        )
    return revokes

import json
import shlex
import time
from typing import List, Tuple

import click
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest

from cli.access_management_config_file_parser import (
    parse_access_management_config,
    AccessManagementConfig,
)
from cli.access_management_rows_generator import (
    generate_access_management_rows,
    AccessManagementRow,
)
from cli.data_masking_config_file_parser import (
    parse_data_masking_config,
    DataMaskingConfig,
)
from cli.data_masking_rows_generator import generate_data_masking_rows, DataMaskingRow
from cli.model import ManifestNode, ModelType

try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType

dbt = dbtRunner()


def get_access_management_rows(
    manifest: Manifest,
    access_management_config: AccessManagementConfig,
    project_name: str,
    database_name: str = None,
) -> List[AccessManagementRow]:
    sql_engine = manifest.metadata.adapter_type
    manifest_nodes = _get_nodes_eligible_for_access_management_from_manifest_file(
        manifest, project_name
    )
    db_name_from_manifest_file = {n.database_name for n in manifest_nodes}
    if len(db_name_from_manifest_file) > 1:
        raise Exception(
            f"Multiple database names found: {', '.join(db_name_from_manifest_file)} in your DBT project.\n"
            f"Most probably you use multi project setup with cross database queries.\n"
            f"Please provide `--database-name` parameter to the command!\n"
        )
    db_name = database_name if database_name else list(db_name_from_manifest_file)[0]
    click.echo(f"Access management will be configured on the: `{db_name}` database")
    for database_access_config in access_management_config.databases_access_config:
        if database_access_config.database_name == db_name:
            return generate_access_management_rows(
                database_access_config, manifest_nodes, project_name, sql_engine
            )
    return []


def get_data_masking_rows(
    manifest: Manifest,
    data_masking_config: DataMaskingConfig,
    project_name: str,
    database_name: str = None,
) -> List[DataMaskingRow]:
    sql_engine = manifest.metadata.adapter_type
    manifest_nodes = _get_nodes_eligible_for_access_management_from_manifest_file(
        manifest, project_name
    )
    db_name_from_manifest_file = {n.database_name for n in manifest_nodes}
    if len(db_name_from_manifest_file) > 1:
        raise Exception(
            f"Multiple database names found: {', '.join(db_name_from_manifest_file)} in your DBT project.\n"
            f"Most probably you use multi project setup with cross database queries.\n"
            f"Please provide `--database-name` parameter to the command!\n"
        )
    db_name = database_name if database_name else list(db_name_from_manifest_file)[0]
    click.echo(f"Access management will be configured on the: `{db_name}` database")

    return generate_data_masking_rows(data_masking_config, manifest_nodes, sql_engine)


def _get_nodes_eligible_for_access_management_from_manifest_file(
    manifest: Manifest, project_name: str
) -> List[ManifestNode]:
    result = []
    for unique_id, node in manifest.nodes.items():
        if (
            unique_id.split(".")[0] == NodeType.Model.value
            and node.config.materialized != "ephemeral"
            and node.package_name == project_name
        ):
            result.append(
                ManifestNode(
                    database_name=node.database,
                    model_type=ModelType.MODEL,
                    model_name=node.name,
                    schema_name=node.schema,
                    materialization=node.config.materialized,
                    path=node.original_file_path,
                )
            )

        if (
            unique_id.split(".")[0] == NodeType.Seed.value
            and node.package_name == project_name
        ):
            result.append(
                ManifestNode(
                    database_name=node.database,
                    model_type=ModelType.SEED,
                    model_name=node.name,
                    schema_name=node.schema,
                    materialization=node.config.materialized,
                    path=node.original_file_path,
                )
            )
    return result


def _build_create_config_table_sql(
    rows: List[AccessManagementRow], table_name: str
) -> str:
    create_table_sql = f"""
CREATE SCHEMA IF NOT EXISTS access_management;
DROP TABLE IF EXISTS access_management.{table_name};
CREATE TABLE access_management.{table_name} (
        project_name TEXT,
        database_name TEXT,
        schema_name TEXT,
        model_name TEXT,
        materialization TEXT,
        identity_type TEXT,
        identity_name TEXT,
        grants SUPER,
        revokes SUPER
    );
    """
    if rows:
        create_table_sql += f"""
        INSERT INTO access_management.{table_name}
        (project_name, database_name, schema_name, model_name, materialization, identity_type, identity_name, grants, revokes)
        VALUES
        """

        values = []
        for row in rows:
            grants = json.dumps(list(row.grants)).replace("'", "''")
            revokes = json.dumps(list(row.revokes)).replace("'", "''")
            value = (
                f"('{row.project_name}', "
                f"'{row.database_name}', "
                f"'{row.schema_name}', "
                f"'{row.model_name}', "
                f"'{row.materialization}', "
                f"'{row.identity_type}', "
                f"'{row.identity_name}', "
                f"'{grants}', "
                f"'{revokes}')"
            )
            values.append(value)

        create_table_sql += ",\n".join(values) + ";"
    return create_table_sql


def _build_create_data_masking_config_table_sql(
    rows: List[DataMaskingRow], table_name: str
) -> str:
    create_table_sql = f"""
CREATE SCHEMA IF NOT EXISTS access_management;
DROP TABLE IF EXISTS access_management.{table_name};
CREATE TABLE access_management.{table_name} (
        database_name TEXT,
        schema_name TEXT,
        model_name TEXT,
        materialization TEXT,
        masking_config SUPER
    );
    """

    if rows:
        create_table_sql += f"""
        INSERT INTO access_management.{table_name}
        (database_name, schema_name, model_name, materialization, masking_config)
        VALUES
        """

        values = []
        for row in rows:
            masking_config = json.dumps(list(row.masking_config)).replace("'", "''")
            value = (
                f"('{row.database_name}', "
                f"'{row.schema_name}', "
                f"'{row.model_name}', "
                f"'{row.materialization}', "
                f"JSON_PARSE('{(masking_config)}'))"
            )
            values.append(value)

        create_table_sql += ",\n".join(values) + ";"
    return create_table_sql


def _get_target_and_vars(command_list: List[str]) -> Tuple[str, str]:
    target = None
    variables = None
    for i in range(len(command_list)):
        if command_list[i] == "--target":
            target = command_list[i + 1] if i + 1 < len(command_list) else None
        elif command_list[i] == "--vars":
            variables = command_list[i + 1] if i + 1 < len(command_list) else None
    return target, variables


def _invoke_compile_command(target: str, variables: str) -> None:
    click.echo("Compiling project...")
    cmd = ["compile"]
    if target:
        cmd.extend(["--target", target])
    if variables:
        cmd.extend(["--vars", variables])
    res = dbt.invoke(cmd)
    if not res.success:
        exit(1)


def load_manifest(manifest_path: str) -> Manifest:
    with open(manifest_path, "r") as file:
        manifest_data = json.load(file)

    return Manifest.from_dict(manifest_data)


def run_configure_access_management_operation(
    temp_config_table_name: str,
    config_table_name: str,
    create_temp_config_table_query: str,
    create_config_table_query: str,
) -> None:
    click.echo("Configuring access management...")
    res = dbt.invoke(
        [
            "run-operation",
            "dbt_access_management.configure_access_management",
            "--args",
            json.dumps(
                {
                    "temp_config_table_name": temp_config_table_name,
                    "config_table_name": config_table_name,
                    "create_temp_config_table_query": create_temp_config_table_query,
                    "create_config_table_query": create_config_table_query,
                }
            ),
        ]
    )
    if not res.success:
        exit(1)


def _invoke_passed_dbt_command(command_list: List[str]) -> None:
    click.echo("Running passed dbt command...")
    res = dbt.invoke(command_list)
    if not res.success:
        exit(1)


@click.command()
@click.option(
    "--dbt-command", help="DBT command you want to execute.", type=str, required=True
)
@click.option(
    "--config-file-path",
    help="Path to the access management config file.",
    type=str,
    default="access_management.yml",
)
@click.option(
    "--database-name",
    help="Database name for which you want to configure access management. "
    "It it highly recommended to specify database name in "
    "multi project setup (for example using meshify or dbt-loom)",
    type=str,
)
@click.option(
    "--config-file-path-data-masking",
    help="Path to the data masking config file.",
    type=str,
    default="data_masking.yml",
)
def dbt_am(
    dbt_command: str,
    config_file_path: str,
    config_file_path_data_masking: str,
    database_name: str = None,
):
    # TODO: Add full-refresh flow with running macros
    #  execute_revoke_all_for_configured_identities and execute_grants_for_configured_identities
    command_list = list(
        filter(lambda c: c.lower() != "dbt", shlex.split(" ".join(dbt_command.split())))
    )
    target, variables = _get_target_and_vars(command_list)
    access_management_config = parse_access_management_config(config_file_path)
    _invoke_compile_command(target, variables)
    data_masking_config = parse_data_masking_config(config_file_path_data_masking)
    _invoke_compile_command(target, variables)
    manifest = load_manifest("target/manifest.json")
    project_name = manifest.metadata.project_name
    access_management_rows = get_access_management_rows(
        manifest, access_management_config, project_name, database_name
    )
    data_masking_rows = get_data_masking_rows(
        manifest, data_masking_config, project_name, database_name
    )
    temp_config_table_name = f"temp_{project_name}_{int(time.time())}_config"
    config_table_name = f"{project_name}_config"
    temp_config_table_query = _build_create_config_table_sql(
        access_management_rows, temp_config_table_name
    )
    config_table_query = _build_create_config_table_sql(
        access_management_rows, config_table_name
    )
    temp_config_data_masking_table_name = (
        f"temp_{project_name}__data_masking_{int(time.time())}_config"
    )
    config_data_masking_table_name = f"{project_name}_data_masking_config"
    temp_config_data_masking_table_query = _build_create_data_masking_config_table_sql(
        data_masking_rows, temp_config_data_masking_table_name
    )
    config_data_masking_table_query = _build_create_data_masking_config_table_sql(
        data_masking_rows, config_data_masking_table_name
    )
    run_configure_access_management_operation(
        temp_config_table_name,
        config_table_name,
        temp_config_table_query,
        config_table_query,
    )
    _invoke_passed_dbt_command(command_list)


if __name__ == "__main__":
    dbt_am()

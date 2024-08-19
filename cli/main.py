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
        entity_type TEXT,
        entity_name TEXT,
        grants SUPER,
        revokes SUPER
    );
        """
    create_table_sql += """
    INSERT INTO access_management.config
    (project_name, database_name, schema_name, model_name, materialization, entity_type, entity_name, grants, revokes)
    VALUES
    """
    values = []
    for row in rows:
        values.append(
            f"('{row.project_name}', '{row.database_name}', '{row.schema_name}', '{row.model_name}', '{row.materialization}', '{row.entity_type}', '{row.entity_name}', '{json.dumps(list(row.grants))}', '{json.dumps(list(row.revokes))}')"
        )
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
    cmd = ["compile"]
    if target:
        cmd.extend(["--target", target])
    if variables:
        cmd.extend(["--vars", variables])
    res = dbt.invoke(cmd)
    if res.exception:
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
    if res.exception:
        exit(1)


def _invoke_passed_dbt_command(command_list: List[str]) -> None:
    res = dbt.invoke(command_list)
    if res.exception:
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
def dbt_am(dbt_command: str, config_file_path: str, database_name: str = None):
    # TODO: Set max-line-length = 240 in .flake8
    command_list = list(
        filter(lambda c: c.lower() != "dbt", shlex.split(" ".join(dbt_command.split())))
    )
    target, variables = _get_target_and_vars(command_list)
    _invoke_compile_command(target, variables)
    manifest = load_manifest("target/manifest.json")
    project_name = manifest.metadata.project_name
    access_management_config = parse_access_management_config(config_file_path)
    access_management_rows = get_access_management_rows(
        manifest, access_management_config, project_name, database_name
    )
    temp_config_table_name = f"temp_{project_name}_{int(time.time())}_config"
    config_table_name = f"{project_name}_config"
    temp_config_table_query = _build_create_config_table_sql(
        access_management_rows, temp_config_table_name
    )
    config_table_query = _build_create_config_table_sql(
        access_management_rows, config_table_name
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

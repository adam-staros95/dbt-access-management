import json
import shlex
from typing import List, Tuple

import click
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest

from dbt_access_management.access_management_config_file_parser import (
    parse_access_management_config,
    AccessManagementConfig,
)
from dbt_access_management.access_management_rows_generator import (
    generate_access_management_rows,
    AccessManagementRow,
)
from dbt_access_management.model import ManifestNode, ModelType

try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType

dbt = dbtRunner()


def generate_create_config_table_query(
    manifest: Manifest,
    access_management_config: AccessManagementConfig,
    database_name: str = None,
) -> str:
    project_name = manifest.metadata.project_name
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
            rows = generate_access_management_rows(
                database_access_config, manifest_nodes, project_name, sql_engine
            )
            return _build_create_config_table_sql(rows)


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


def _build_create_config_table_sql(rows: List[AccessManagementRow]) -> str:
    create_table_sql = """
CREATE SCHEMA IF NOT EXISTS access_management;
DROP TABLE IF EXISTS access_management.config;
CREATE TABLE access_management.config (
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


def run_create_create_access_management_table_operation(query: str) -> None:
    res = dbt.invoke(
        [
            "run-operation",
            "create_access_management_table",
            "--args",
            json.dumps({"create_access_management_table_query": query}),
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
def cli(dbt_command: str, config_file_path: str, database_name: str = None):
    # TODO: Set max-line-length = 240 in .flake8
    command_list = list(
        filter(lambda c: c.lower() != "dbt", shlex.split(" ".join(dbt_command.split())))
    )
    target, variables = _get_target_and_vars(command_list)
    _invoke_compile_command(target, variables)
    manifest = load_manifest("target/manifest.json")
    access_management_config = parse_access_management_config(config_file_path)
    query = generate_create_config_table_query(
        manifest, access_management_config, database_name
    )
    run_create_create_access_management_table_operation(query)
    _invoke_passed_dbt_command(command_list)


if __name__ == "__main__":
    cli()

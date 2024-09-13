import json
import os
import shlex
from typing import List

import click
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest

from cli.access_mangement.configure_access_management_macro_properties_provider import (
    get_configure_access_management_macro_properties,
)
from cli.constants import SUPPORTED_SQL_ENGINES
from cli.data_masking.configure_data_masking_macro_properties_provider import (
    get_configure_data_masking_macro_properties,
)
from cli.exceptions import (
    MultipleDatabaseNamesException,
    SQLEngineNotSupportedException,
)
from cli.model import ManifestNode, ModelType, ConfigureMacroProperties

try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType

dbt = dbtRunner()


def _get_manifest_nodes_eligible_for_configuration(
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
                    path=node.original_file_path
                    if os.name != "nt"
                    else node.original_file_path.replace("\\", "/"),
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
                    path=node.original_file_path
                    if os.name != "nt"
                    else node.original_file_path.replace("\\", "/"),
                )
            )
    return result


def _get_database_name(manifest_nodes: List[ManifestNode], database_name: str = None):
    db_name_from_manifest_file = {n.database_name for n in manifest_nodes}
    if len(db_name_from_manifest_file) > 1:
        raise MultipleDatabaseNamesException(db_name_from_manifest_file)
    return database_name if database_name else list(db_name_from_manifest_file)[0]


def _invoke_compile_command(command_list: List[str]) -> None:
    click.echo("Compiling project...")

    target = next(
        (
            command_list[i + 1]
            for i in range(len(command_list) - 1)
            if command_list[i] == "--target"
        ),
        None,
    )
    variables = next(
        (
            command_list[i + 1]
            for i in range(len(command_list) - 1)
            if command_list[i] == "--vars"
        ),
        None,
    )

    cmd = ["compile"]
    if target:
        cmd.extend(["--target", target])
    if variables:
        cmd.extend(["--vars", variables])
    res = dbt.invoke(cmd)
    if not res.success:
        exit(1)


def load_manifest(manifest_path: str = "target/manifest.json") -> Manifest:
    with open(manifest_path, "r") as file:
        manifest_data = json.load(file)

    return Manifest.from_dict(manifest_data)


def run_configure_macro(
    configure_access_management_macro_properties: ConfigureMacroProperties = None,
    configure_data_masking_macro_properties: ConfigureMacroProperties = None,
) -> None:
    def prepare_access_management_args(
        configure_properties: ConfigureMacroProperties,
    ) -> dict:
        return {
            "temp_access_management_config_table_name": configure_properties.temp_config_table_name,
            "config_access_management_table_name": configure_properties.config_table_name,
            "create_temp_access_management_config_table_query": configure_properties.create_temp_config_table_query,
            "create_access_management_config_table_query": configure_properties.create_config_table_query,
        }

    def prepare_data_masking_args(
        configure_properties: ConfigureMacroProperties,
    ) -> dict:
        return {
            "temp_data_masking_config_table_name": configure_properties.temp_config_table_name,
            "config_data_masking_table_name": configure_properties.config_table_name,
            "create_temp_data_masking_config_table_query": configure_properties.create_temp_config_table_query,
            "create_data_masking_config_table_query": configure_properties.create_config_table_query,
        }

    def run_dbt_operation(operation_name: str, args: dict) -> None:
        res = dbt.invoke(
            [
                "run-operation",
                operation_name,
                "--args",
                json.dumps(args),
            ]
        )
        if not res.success:
            exit(1)

    if (
        configure_access_management_macro_properties
        and configure_data_masking_macro_properties
    ):
        click.echo("Configuring access management and data masking...")
        combined_args = {
            **prepare_access_management_args(
                configure_access_management_macro_properties
            ),
            **prepare_data_masking_args(configure_data_masking_macro_properties),
        }
        run_dbt_operation("dbt_access_management.configure", combined_args)

    else:
        if configure_access_management_macro_properties:
            click.echo("Configuring access management...")
            access_management_args = prepare_access_management_args(
                configure_access_management_macro_properties
            )
            run_dbt_operation(
                "dbt_access_management.configure_access_management",
                access_management_args,
            )

        if configure_data_masking_macro_properties:
            click.echo("Configuring data masking...")
            data_masking_args = prepare_data_masking_args(
                configure_data_masking_macro_properties
            )
            run_dbt_operation(
                "dbt_access_management.configure_data_masking", data_masking_args
            )


def _invoke_passed_dbt_command(command_list: List[str]) -> None:
    click.echo("Running passed dbt command...")
    res = dbt.invoke(command_list)
    if not res.success:
        exit(1)


@click.group()
def cli():
    pass


@click.command()
@click.option(
    "--dbt-command", help="DBT command you want to execute.", type=str, required=True
)
@click.option(
    "--configure-access-management",
    help="Set to false to disable access management configuration",
    type=bool,
    required=True,
    default=True,
)
@click.option(
    "--configure-data-masking",
    help="Set to false to disable data masking configuration",
    type=bool,
    required=True,
    default=True,
)
@click.option(
    "--access-management-config-file-path",
    help="Path to the access management config file.",
    type=str,
    default="access_management.yml",
)
@click.option(
    "--data-masking-config-file-path",
    help="Path to the data masking config file.",
    type=str,
    default="data_masking.yml",
)
@click.option(
    "--database-name",
    help="Database name for which you want to configure access management. "
    "It it required to specify database name in "
    "multi project setup (for example using meshify or dbt-loom)",
    type=str,
)
def configure(
    dbt_command: str,
    configure_access_management: bool,
    configure_data_masking: bool,
    access_management_config_file_path: str,
    data_masking_config_file_path: str,
    database_name: str = None,
):
    command_list = list(
        filter(lambda c: c.lower() != "dbt", shlex.split(" ".join(dbt_command.split())))
    )
    _invoke_compile_command(command_list)

    manifest = load_manifest()
    project_name = manifest.metadata.project_name
    sql_engine = manifest.metadata.adapter_type
    if sql_engine.lower() not in SUPPORTED_SQL_ENGINES:
        raise SQLEngineNotSupportedException()

    manifest_nodes = _get_manifest_nodes_eligible_for_configuration(
        manifest, project_name
    )
    database_name = _get_database_name(manifest_nodes, database_name)
    click.echo(f"Running dbt-access-management configurations on {database_name} database...")

    configure_access_management_macro_properties = (
        (
            get_configure_access_management_macro_properties(
                manifest_nodes=manifest_nodes,
                config_file_path=access_management_config_file_path,
                sql_engine=sql_engine,
                database_name=database_name,
                project_name=project_name,
            )
        )
        if configure_access_management
        else None
    )

    configure_data_masking_macro_properties = (
        (
            get_configure_data_masking_macro_properties(
                manifest_nodes=manifest_nodes,
                config_file_path=data_masking_config_file_path,
                project_name=project_name,
            )
        )
        if configure_data_masking
        else None
    )

    run_configure_macro(
        configure_access_management_macro_properties=configure_access_management_macro_properties,
        configure_data_masking_macro_properties=configure_data_masking_macro_properties,
    )

    _invoke_passed_dbt_command(command_list)


cli.add_command(configure)

if __name__ == "__main__":
    cli()

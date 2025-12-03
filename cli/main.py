import json
import os
import shlex
from typing import List, Optional

import click
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest

from cli.access_mangement.configure_access_management_macro_properties_provider import (
    get_configure_access_management_macro_properties,
)
from cli.constants import (
    SUPPORTED_SQL_ENGINES,
    DEFAULT_SCHEMA_NAME,
    DEFAULT_ACCESS_MANAGEMENT_CONFIG_FILE_PATH,
    DEFAULT_DATA_MASKING_CONFIG_FILE_PATH,
)
from cli.data_masking.configure_data_masking_macro_properties_provider import (
    get_configure_data_masking_macro_properties,
)
from cli.exceptions import (
    SQLEngineNotSupportedException,
)
from cli.model import ManifestNode, ModelType, ConfigureMacroProperties

try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType

dbt = dbtRunner()


def _get_command_list(dbt_command: str) -> List[str]:
    return list(
        filter(lambda c: c.lower() != "dbt", shlex.split(" ".join(dbt_command.split())))
    )


def _get_variables(command_list: List[str]) -> Optional[str]:
    return next(
        (
            command_list[i + 1]
            for i in range(len(command_list) - 1)
            if command_list[i] == "--vars"
        ),
        None,
    )


def _get_target(command_list: List[str]) -> Optional[str]:
    return next(
        (
            command_list[i + 1]
            for i in range(len(command_list) - 1)
            if command_list[i] == "--target"
        ),
        None,
    )


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
                    alias=node.alias,
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
                    alias=node.alias,
                    model_name=node.name,
                    schema_name=node.schema,
                    materialization=node.config.materialized,
                    path=node.original_file_path
                    if os.name != "nt"
                    else node.original_file_path.replace("\\", "/"),
                )
            )

        if (
            unique_id.split(".")[0] == NodeType.Snapshot.value
            and node.package_name == project_name
        ):
            result.append(
                ManifestNode(
                    database_name=node.database,
                    model_type=ModelType.SNAPSHOT,
                    model_name=node.name,
                    schema_name=node.schema,
                    materialization=node.config.materialized,
                    path=node.original_file_path
                    if os.name != "nt"
                    else node.original_file_path.replace("\\", "/"),
                    alias=node.alias,
                )
            )
    return result


def _invoke_compile_command(target: str = None, variables: str = None) -> None:
    click.echo("Compiling project...")
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
    target: str = None,
    variables: str = None,
) -> None:
    def prepare_access_management_args(
        configure_properties: ConfigureMacroProperties,
    ) -> dict:
        return {
            "temp_access_management_config_table_name": configure_properties.temp_config_table_name,
            "config_access_management_table_name": configure_properties.config_table_name,
            "create_temp_access_management_config_table_query": configure_properties.create_temp_config_table_query,
            "create_access_management_config_table_query": configure_properties.create_config_table_query,
            "access_management_database_name": configure_properties.database_name,
            "access_management_schema_name": configure_properties.schema_name,
        }

    def prepare_data_masking_args(
        configure_properties: ConfigureMacroProperties,
    ) -> dict:
        return {
            "temp_data_masking_config_table_name": configure_properties.temp_config_table_name,
            "config_data_masking_table_name": configure_properties.config_table_name,
            "create_temp_data_masking_config_table_query": configure_properties.create_temp_config_table_query,
            "create_data_masking_config_table_query": configure_properties.create_config_table_query,
            "access_management_database_name": configure_properties.database_name,
            "access_management_schema_name": configure_properties.schema_name,
        }

    def run_dbt_operation(operation_name: str, args: dict) -> None:
        cmd = [
            "run-operation",
            operation_name,
            "--args",
            json.dumps(args),
        ]
        if target:
            cmd.extend(["--target", target])
        if variables:
            cmd.extend(["--vars", variables])
        res = dbt.invoke(cmd)
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
    "--access-management-database-name",
    help="Database name for storing access management configuration",
    type=str,
    required=True,
)
@click.option(
    "--access-management-schema-name",
    help="Schema name for storing access management configuration,`access_management` by default.",
    type=str,
    required=True,
    default=DEFAULT_SCHEMA_NAME,
)
@click.option(
    "--access-management-config-file-path",
    help="Path to the access management config file.",
    type=str,
    default=DEFAULT_ACCESS_MANAGEMENT_CONFIG_FILE_PATH,
)
@click.option(
    "--data-masking-config-file-path",
    help="Path to the data masking config file.",
    type=str,
    default=DEFAULT_DATA_MASKING_CONFIG_FILE_PATH,
)
def configure(
    dbt_command: str,
    configure_access_management: bool,
    configure_data_masking: bool,
    access_management_database_name: str,
    access_management_schema_name: str,
    access_management_config_file_path: str,
    data_masking_config_file_path: str,
):
    command_list = _get_command_list(dbt_command)
    target = _get_target(command_list)
    variables = _get_variables(command_list)
    _invoke_compile_command(target, variables)

    manifest = load_manifest()
    project_name = manifest.metadata.project_name
    sql_engine = manifest.metadata.adapter_type
    if sql_engine.lower() not in SUPPORTED_SQL_ENGINES:
        raise SQLEngineNotSupportedException()

    manifest_nodes = _get_manifest_nodes_eligible_for_configuration(
        manifest, project_name
    )

    configure_access_management_macro_properties = (
        (
            get_configure_access_management_macro_properties(
                manifest_nodes=manifest_nodes,
                config_file_path=access_management_config_file_path,
                sql_engine=sql_engine,
                project_name=project_name,
                database_name=access_management_database_name,
                schema_name=access_management_schema_name,
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
                sql_engine=sql_engine,
                project_name=project_name,
                database_name=access_management_database_name,
                schema_name=access_management_schema_name,
            )
        )
        if configure_data_masking
        else None
    )

    run_configure_macro(
        configure_access_management_macro_properties=configure_access_management_macro_properties,
        configure_data_masking_macro_properties=configure_data_masking_macro_properties,
        target=target,
        variables=variables,
    )
    _invoke_passed_dbt_command(command_list)


cli.add_command(configure)

if __name__ == "__main__":
    cli()

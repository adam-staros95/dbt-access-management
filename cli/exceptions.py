from typing import Set

from cli.constants import SUPPORTED_SQL_ENGINES


class MultipleDatabaseNamesException(Exception):
    def __init__(self, db_names: Set[str]):
        message = (
            f"Multiple database names found: {', '.join(db_names)} in your DBT project.\n"
            f"Most probably you use multi project setup with cross database queries.\n"
            f"Please provide `--database-name` parameter to the command!\n"
        )
        super().__init__(message)


class DatabaseAccessManagementConfigNotExistsException(Exception):
    def __init__(self, db_name: str):
        message = f"Access management config for database: {db_name} not specified!"
        super().__init__(message)


class SQLEngineNotSupportedException(Exception):
    def __init__(self):
        message = (
            f"Currently supported sql engines are: {', '.join(SUPPORTED_SQL_ENGINES)}"
        )
        super().__init__(message)


class AccessManagementConfigFileNotFoundException(Exception):
    def __init__(self, file_path: str):
        message = (
            f"Access management configuration file not found in path: {file_path}."
        )
        super().__init__(message)


class DataMaskingConfigFileNotFoundException(Exception):
    def __init__(self, file_path: str):
        message = f"Data masking configuration file not found in path: {file_path}."
        super().__init__(message)


class OverridingSchemaNameNotSupportedException(Exception):
    def __init__(self):
        message = "Overriding schema name is not supported in Redshift!. Support will be added in next releases."
        super().__init__(message)


class NotSupportedMaterializationException(Exception):
    def __init__(
        self, provided_materialization: str, supported_materializations: list[str]
    ):
        message = (
            f"Materialization: {provided_materialization} is not supported!\n"
            f"Supported materializations are: {', '.join(supported_materializations)}.\n"
        )
        super().__init__(message)

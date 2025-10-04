from cli.constants import SUPPORTED_SQL_ENGINES


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


class NotSupportedMaterializationException(Exception):
    def __init__(
        self, provided_materialization: str, supported_materializations: list[str]
    ):
        message = (
            f"Materialization: {provided_materialization} is not supported!\n"
            f"Supported materializations are: {', '.join(supported_materializations)}.\n"
        )
        super().__init__(message)


class DatabricksColumnMaskingNotSupportedOnViewException(Exception):
    def __init__(self, model_name: str):
        message = (
            f"Databricks does not support column masking on views, "
            f"but masking configured for following model materialized as view: {model_name}.\n"
            f"Remove masking config from view!"
        )
        super().__init__(message)

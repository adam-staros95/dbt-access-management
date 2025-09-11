class WorkspaceNameNotProvidedException(Exception):
    def __init__(self):
        message = (
            "Workspace name parameter is required in Databricks!\n"
            "It is used to parse proper config from access management configuration file.\n"
            "Please provide it using `--workspace-name` parameter."
        )
        super().__init__(message)


class CatalogNameNotProvidedException(Exception):
    def __init__(self):
        message = (
            "Catalog name parameter is required in Databricks!\n"
            "It is used to determine in which catalog access management should be configured.\n"
            "Please provide it using `--catalog-name` parameter."
        )
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

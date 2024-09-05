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

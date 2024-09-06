from enum import Enum
from typing import List


class SQLEngine(str, Enum):
    REDSHIFT = "redshift"


SUPPORTED_SQL_ENGINES: List[SQLEngine] = [SQLEngine.REDSHIFT]

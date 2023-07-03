"""GOB Prepare importer typing.

See GOB-Prepare/src/data/*.json
"""


from typing import Literal, TypedDict

from gobprepare.typing import ActionCommonConfig, SQLBaseConfig


class APIImporterConfig(SQLBaseConfig):
    """SqlAPIImporter action configuration."""

    destination: str
    meta_type: str  # bag_verblijfsobjectenRootObjectType
    schema: str
    type: Literal["import_api"]


class ReadConfig(TypedDict):
    """Read file configuration."""

    file_filter: str
    filter_list: list

class SqlCsvImporterConfig(ActionCommonConfig, total=False):
    """SqlCsvImporter action configuration."""

    column_names: dict[str, str]
    description: str
    destination: str
    encoding: Literal["utf-8", "iso-8859-1"]
    objectstore: str
    read_config: ReadConfig
    separator: Literal[";", ",", "|", "\t"]
    # URL
    source: str
    type: Literal["import_csv"]

class SqlDumpImporterConfig(ActionCommonConfig, total=False):
    # TODO: check the given param should be SQLBaseConfig or ExecuteSQLConfig or ActionCommonConfig
    """SqlDumpImporter action configuration."""

    description: str
    destination: str
    encoding: Literal["utf-8", "iso-8859-1"]
    objectstore: str
    read_config: ReadConfig
    # URL
    source: str
    type: Literal["import_sqlDump"]
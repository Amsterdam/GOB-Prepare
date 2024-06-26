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
    container: str
    filter_list: str
    substitution: dict[str, str]
    comments_regexp: str
    split_regexp: str
    data_delimiter_regexp: str
    copy_query_regex: str


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
    """SqlDumpImporter action configuration."""

    description: str
    destination: str
    encoding: Literal["utf-8", "iso-8859-1"]
    objectstore: str
    read_config: ReadConfig
    source: str
    type: Literal["import_sqlDump"]

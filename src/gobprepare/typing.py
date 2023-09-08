"""GOB Prepare typing."""


from collections.abc import Collection
from typing import Any, Literal, TypedDict


class PrepareMapping(TypedDict):
    """Prepare mapping."""

    # See *Config TypedDicts below.
    actions: list[dict[str, Any]]
    catalogue: str
    destination: dict[str, str]
    name: str
    source: dict[str, str]
    version: str


class TaskOverride(TypedDict, total=False):
    """Task Override."""

    ignore: list[str]
    include: list[str]
    type: str


Summary = dict[str, Collection[str]]


class Message(TypedDict, total=False):
    """Message."""

    contents: dict[str, str]
    header: dict[str, Any]
    original_action: str
    override: TaskOverride
    summary: Summary


class Task(TypedDict, total=False):
    """Task configuration."""

    task_name: str
    dependencies: list[str]
    extra_msg: Message


TaskList = list[Task]


class ClearConfig(TypedDict):
    """Clear action configuration."""

    id: Literal["clear_schemas"]
    schemas: list[str]
    type: Literal["clear"]


class ActionCommonConfig(TypedDict):
    """Action base configuration."""

    depends_on: list[str]
    id: str


class SQLBaseConfig(ActionCommonConfig):
    """SQL action base configuration."""

    description: str
    query_src: Literal["file", "string"]
    query: str


class ExecuteSQLConfig(SQLBaseConfig):
    """Execute SQL action configuration."""

    type: Literal["execute_sql"]


class CreateTableConfig(SQLBaseConfig):
    """Create Table action configuration."""

    table_name: str
    type: Literal["create_table"]


class PublishSchemasConfig(TypedDict):
    """Publish Schemas action configuration."""

    depends_on: Literal["*"]
    id: str
    publish_schemas: dict[str, str]
    type: Literal["publish_schemas"]


class RowCountConfig(ActionCommonConfig):
    """Check table row counts action configuration."""

    description: str
    table_row_counts: dict[str, int]
    margin_percentage: int
    type: Literal["check_row_counts"]

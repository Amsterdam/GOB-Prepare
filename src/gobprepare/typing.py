"""GOB Prepare typing."""


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


class ActionCommonConfig(TypedDict):
    """Action base configuration."""

    depends_on: list[str]
    id: str


class SQLBaseConfig(ActionCommonConfig):
    """SQL action base configuration."""

    description: str
    query_src: Literal["file", "string"]
    query: str

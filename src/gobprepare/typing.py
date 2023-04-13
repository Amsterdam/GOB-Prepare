"""GOB Prepare typing."""


from typing import Any, TypedDict


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

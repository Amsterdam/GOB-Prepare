"""GOB Prepare typing."""


from typing import TypedDict


class ActionCommonConfig(TypedDict):
    """Action base configuration."""

    depends_on: list[str]
    id: str

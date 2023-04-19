"""GOB Prepare utils typing."""


from typing import Any, TypedDict


class FieldType(TypedDict):
    """Type of a meta field."""

    name: str


class MetaField(TypedDict):
    """Meta field."""

    name: str
    description: str
    type: FieldType


class GraphQLEntities(TypedDict):
    """GraphQL entities."""

    # GraphQL metadata
    data: dict[str, Any]
    # GraphQL node data
    node: dict[str, str]

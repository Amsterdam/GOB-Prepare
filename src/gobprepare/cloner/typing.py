"""GOB Prepare cloner typing.

See GOB-Prepare/src/data/*.json
"""


from typing import Literal, TypedDict, Union


class ClonerConfig(TypedDict):
    """Cloner action configuration."""

    depends_on: list[str]
    destination_schema: str
    id: str
    # id_columns[key] is type list[str], only id_columns["_defaults"] is type list[list[str]]
    id_columns: dict[str, list[Union[str, list[str]]]]
    ignore: list[str]
    include: list[str]
    source_schema: str
    type: Literal["clone"]

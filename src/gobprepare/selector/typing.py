"""GOB Prepare selector typing.

See GOB-Prepare/src/data/*.json
"""


from os import PathLike
from typing import Literal, TypedDict, Union

from gobprepare.typing import ActionCommonConfig


class DestinationTable(TypedDict):
    """Destination table mapping."""

    name: str
    create: bool
    columns: list[dict[str, str]]


class SelectorConfig(ActionCommonConfig):
    """Selector action configuration."""

    destination_table: DestinationTable
    ignore_missing: bool
    query_src: Literal["file", "string"]
    query: Union[list[str], str, PathLike[str]]
    source: Literal["dst", "src"]
    type: Literal["select"]

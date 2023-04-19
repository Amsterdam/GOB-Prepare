import argparse
import sys
from typing import Literal, Union

from gobcore import standalone
from gobcore.message_broker.config import (
    PREPARE_COMPLETE_QUEUE,
    PREPARE_QUEUE,
    PREPARE_RESULT_KEY,
    PREPARE_TASK_QUEUE,
    PREPARE_TASK_RESULT_KEY,
    TASK_REQUEST_KEY,
    WORKFLOW_EXCHANGE,
)
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.message_broker.typing import ServiceDefinition

from gobprepare.mapping import get_mapping, get_prepare_definition_file_location
from gobprepare.prepare_client import PrepareClient
from gobprepare.typing import Message, Summary


def _prepare_client_for_msg(msg: Message) -> PrepareClient:
    assert "catalogue" in msg.get("header", {})
    mapping_file = get_prepare_definition_file_location(msg["header"]["catalogue"])
    prepare_config = get_mapping(mapping_file)

    return PrepareClient(prepare_config=prepare_config, msg=msg)


def handle_prepare_msg(msg: Message) -> Message:
    """Handle prepare message."""
    prepare_client = _prepare_client_for_msg(msg)
    return prepare_client.start_prepare_process()


def handle_prepare_step_msg(msg: Message) -> Union[Message, Literal[False]]:
    """Handle prepare step message."""
    prepare_client = _prepare_client_for_msg(msg)
    return prepare_client.run_prepare_task()


def handle_prepare_complete_msg(msg: Message) -> Summary:
    """Handle prepare complete message."""
    prepare_client = _prepare_client_for_msg(msg)
    return prepare_client.complete_prepare_process()


SERVICE_DEFINITION: ServiceDefinition = {
    "prepare_request": {
        "queue": PREPARE_QUEUE,
        "handler": handle_prepare_msg,
        "report": {
            "key": TASK_REQUEST_KEY,
            "exchange": WORKFLOW_EXCHANGE,
        },
    },
    "prepare_task": {
        "queue": PREPARE_TASK_QUEUE,
        "handler": handle_prepare_step_msg,
        "report": {
            "key": PREPARE_TASK_RESULT_KEY,
            "exchange": WORKFLOW_EXCHANGE,
        },
        "pass_args_standalone": [
            "task_name",
        ],
    },
    "prepare_complete": {
        "queue": PREPARE_COMPLETE_QUEUE,
        "handler": handle_prepare_complete_msg,
        "report": {
            "key": PREPARE_RESULT_KEY,
            "exchange": WORKFLOW_EXCHANGE,
        },
    },
}


def argument_parser() -> argparse.ArgumentParser:
    """Parse arguments for the prepare handler in standalone mode."""
    parser: argparse.ArgumentParser
    parser, subparsers = standalone.parent_argument_parser()

    prepare_task = subparsers.add_parser(name="prepare_task", description="Execute a prepare task")
    prepare_task.add_argument("--catalogue", required=True, help="The name of the data catalogue")
    prepare_task.add_argument("--task_name", required=True, help="The task to execute")

    return parser


def main() -> None:
    """Determine prepare mode: messagedriven_service or standalone."""
    if len(sys.argv) == 1:
        print("No arguments found, wait for messages on the message broker.")
        messagedriven_service(SERVICE_DEFINITION, "Prepare")
    else:
        print("Arguments found, run as standalone")
        parser = argument_parser()
        args = parser.parse_args()
        sys.exit(standalone.run_as_standalone(args, SERVICE_DEFINITION))


if __name__ == "__main__":
    main()  # pragma: no cover

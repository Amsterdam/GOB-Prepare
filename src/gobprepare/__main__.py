from gobcore.message_broker.config import WORKFLOW_EXCHANGE, PREPARE_QUEUE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobprepare.prepare_client import PrepareClient
from gobprepare.mapping import get_mapping


def _prepare_client_for_msg(msg):
    assert('prepare_config' in msg)
    prepare_config = get_mapping(msg['prepare_config'])
    return PrepareClient(prepare_config=prepare_config, msg=msg)


def handle_prepare_msg(msg):
    prepare_client = _prepare_client_for_msg(msg)
    prepare_client.start_prepare_process()


def handle_prepare_step_msg(msg):
    prepare_client = _prepare_client_for_msg(msg)
    return prepare_client.run_prepare_task()


def handle_prepare_complete_msg(msg):
    prepare_client = _prepare_client_for_msg(msg)
    return prepare_client.complete_prepare_process()


SERVICE_DEFINITION = {
    'prepare_request': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': PREPARE_QUEUE,
        'key': 'prepare.start',
        'handler': handle_prepare_msg,
    },
    'prepare_task': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': PREPARE_QUEUE,
        'key': 'prepare.task',
        'report': {
            'key': 'task.complete',
            'exchange': WORKFLOW_EXCHANGE,
        },
        'handler': handle_prepare_step_msg,
    },
    'prepare_complete': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': PREPARE_QUEUE,
        'key': 'prepare.complete',
        'report': {
            'key': 'import.result',
            'exchange': WORKFLOW_EXCHANGE,
        },
        'handler': handle_prepare_complete_msg,
    }
}


def init():
    if __name__ == "__main__":
        messagedriven_service(SERVICE_DEFINITION, "Prepare")


init()

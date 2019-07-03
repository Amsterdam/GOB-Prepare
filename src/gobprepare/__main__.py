from gobcore.message_broker.config import WORKFLOW_EXCHANGE, PREPARE_QUEUE, PREPARE_RESULT_KEY, \
    PREPARE_TASK_RESULT_KEY, PREPARE_TASK_QUEUE, PREPARE_COMPLETE_QUEUE, TASK_REQUEST_KEY
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobprepare.prepare_client import PrepareClient
from gobprepare.mapping import get_mapping, get_prepare_definition_file_location


def _prepare_client_for_msg(msg):
    assert 'catalogue' in msg.get('header', {})
    mapping_file = get_prepare_definition_file_location(msg['header']['catalogue'])
    prepare_config = get_mapping(mapping_file)

    return PrepareClient(prepare_config=prepare_config, msg=msg)


def handle_prepare_msg(msg):
    prepare_client = _prepare_client_for_msg(msg)
    return prepare_client.start_prepare_process()


def handle_prepare_step_msg(msg):
    prepare_client = _prepare_client_for_msg(msg)
    return prepare_client.run_prepare_task()


def handle_prepare_complete_msg(msg):
    prepare_client = _prepare_client_for_msg(msg)
    return prepare_client.complete_prepare_process()


SERVICE_DEFINITION = {
    'prepare_request': {
        'queue': PREPARE_QUEUE,
        'handler': handle_prepare_msg,
        'report': {
            'key': TASK_REQUEST_KEY,
            'exchange': WORKFLOW_EXCHANGE,
        }
    },
    'prepare_task': {
        'queue': PREPARE_TASK_QUEUE,
        'handler': handle_prepare_step_msg,
        'report': {
            'key': PREPARE_TASK_RESULT_KEY,
            'exchange': WORKFLOW_EXCHANGE,
        },
    },
    'prepare_complete': {
        'queue': PREPARE_COMPLETE_QUEUE,
        'handler': handle_prepare_complete_msg,
        'report': {
            'key': PREPARE_RESULT_KEY,
            'exchange': WORKFLOW_EXCHANGE,
        },
    },
}


def init():
    if __name__ == "__main__":
        messagedriven_service(SERVICE_DEFINITION, "Prepare")


init()

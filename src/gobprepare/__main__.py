from gobcore.message_broker.config import WORKFLOW_EXCHANGE, PREPARE_QUEUE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobprepare.prepare_client import PrepareClient
from gobprepare.mapping import get_mapping


def handle_prepare_msg(msg):
    assert('prepare_config' in msg)
    prepare_config = get_mapping(msg['prepare_config'])
    prepare_client = PrepareClient(prepare_config=prepare_config, msg=msg)
    prepare_client.start_prepare_process()
    return prepare_client.get_result()


SERVICE_DEFINITION = {
    'prepare_request': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': PREPARE_QUEUE,
        'report': {
            'key': 'import.start',
            'exchange': WORKFLOW_EXCHANGE,
        },
        'key': 'prepare.start',
        'handler': handle_prepare_msg,
    }
}


def init():
    if __name__ == "__main__":
        messagedriven_service(SERVICE_DEFINITION, "Prepare")


init()

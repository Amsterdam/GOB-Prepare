from gobcore.message_broker.config import WORKFLOW_EXCHANGE, PREPARE_QUEUE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobprepare.prepare_client import PrepareClient
from gobprepare.mapping import get_mapping


def handle_prepare_msg(msg):
    assert('dataset_file' in msg)
    dataset = get_mapping(msg['dataset_file'])
    prepare_client = PrepareClient(dataset=dataset, msg=msg)
    prepare_client.start_prepare_process()


SERVICE_DEFINITION = {
    'prepare_request': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': PREPARE_QUEUE,
        'key': 'prepare.start',
        'handler': handle_prepare_msg,
    }
}


def init():
    if __name__ == "__main__":
        messagedriven_service(SERVICE_DEFINITION, "Prepare")


init()

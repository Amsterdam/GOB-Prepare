from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobprepare.__main__ import handle_prepare_msg, SERVICE_DEFINITION


class TestMain(TestCase):

    def setUp(self):
        self.mock_msg = {
            'prepare_config': 'data/somefile.json',
        }

    @patch("gobprepare.__main__.PrepareClient")
    @patch("gobprepare.__main__.get_mapping")
    def test_handle_prepare_msg(self, mock_get_mapping, mock_prepare_client):
        mock_prepare_client_instance = MagicMock()
        mock_prepare_client.return_value = mock_prepare_client_instance
        mock_get_mapping.return_value = "mapped_file"
        handle_prepare_msg(self.mock_msg)

        mock_prepare_client.assert_called_with(prepare_config="mapped_file", msg=self.mock_msg)
        mock_prepare_client_instance.start_prepare_process.assert_called_once()

    def test_handle_prepare_msg_without_dataset(self):
        del self.mock_msg['prepare_config']

        with self.assertRaises(AssertionError):
            handle_prepare_msg(self.mock_msg)

    @patch("gobprepare.__main__.messagedriven_service")
    def test_main_entry(self, mock_messagedriven_service):
        from gobprepare import __main__ as module
        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_messagedriven_service.assert_called_with(SERVICE_DEFINITION, "Prepare")

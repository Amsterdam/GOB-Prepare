import sys
from unittest import TestCase
from unittest.mock import patch, MagicMock, ANY

from gobprepare.__main__ import _prepare_client_for_msg, handle_prepare_complete_msg, handle_prepare_msg, \
    handle_prepare_step_msg, SERVICE_DEFINITION, main


class TestMain(TestCase):

    def setUp(self):
        self.mock_msg = {
            'header': {
                'catalogue': 'somecat'
            },
        }

    @patch("gobprepare.__main__.PrepareClient")
    @patch("gobprepare.__main__.get_mapping")
    @patch("gobprepare.__main__.get_prepare_definition_file_location")
    def test_prepare_client_for_msg(self, mock_file_location, mock_get_mapping, mock_prepare_client):
        mock_get_mapping.return_value = "mapped_file"
        result = _prepare_client_for_msg(self.mock_msg)
        mock_file_location.assert_called_with('somecat')
        mock_get_mapping.assert_called_with(mock_file_location.return_value)

        mock_prepare_client.assert_called_with(prepare_config="mapped_file", msg=self.mock_msg)
        self.assertEqual(mock_prepare_client.return_value, result)

    def test_prepare_client_for_msg_without_dataset(self):
        del self.mock_msg['header']['catalogue']

        with self.assertRaises(AssertionError):
            _prepare_client_for_msg(self.mock_msg)

    @patch("gobprepare.__main__._prepare_client_for_msg")
    def test_handle_prepare_msg(self, mock_prepare_client_for_msg):
        mock_prepare_client_for_msg.return_value = MagicMock()
        msg = {'some': 'msg'}
        result = handle_prepare_msg(msg)
        mock_prepare_client_for_msg.assert_called_with(msg)
        mock_prepare_client_for_msg.return_value.start_prepare_process.assert_called_once()
        self.assertEqual(mock_prepare_client_for_msg.return_value.start_prepare_process.return_value, result)

    @patch("gobprepare.__main__._prepare_client_for_msg")
    def test_handle_prepare_step_msg(self, mock_prepare_client_for_msg):
        mock_prepare_client_for_msg.return_value = MagicMock()
        msg = {'some': 'msg'}
        result = handle_prepare_step_msg(msg)
        mock_prepare_client_for_msg.assert_called_with(msg)
        mock_prepare_client_for_msg.return_value.run_prepare_task.assert_called_once()
        self.assertEqual(mock_prepare_client_for_msg.return_value.run_prepare_task.return_value, result)

    @patch("gobprepare.__main__._prepare_client_for_msg")
    def test_handle_prepare_complete_msg(self, mock_prepare_client_for_msg):
        mock_prepare_client_for_msg.return_value = MagicMock()
        msg = {'some': 'msg'}
        result = handle_prepare_complete_msg(msg)
        mock_prepare_client_for_msg.assert_called_with(msg)
        mock_prepare_client_for_msg.return_value.complete_prepare_process.assert_called_once()
        self.assertEqual(mock_prepare_client_for_msg.return_value.complete_prepare_process.return_value, result)

    @patch("gobprepare.__main__.messagedriven_service")
    def test_main_entry(self, mock_messagedriven_service):
        sys.argv = ['python -m gobprepare']
        main()
        mock_messagedriven_service.assert_called_with(SERVICE_DEFINITION, "Prepare")

    @patch("gobprepare.__main__.standalone.run_as_standalone", return_value=0)
    def test_main_standalone(self, mock_run_as_standalone):
        sys.argv = ['python -m gobprepare', 'prepare_task', '--catalogue', 'bogus', '--task_name', 'some_task']

        with self.assertRaisesRegex(SystemExit, "0"):
            main()

        mock_run_as_standalone.assert_called_with(ANY, SERVICE_DEFINITION)

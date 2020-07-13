import os
import mock
import json
import unittest
import responses
import settings
from IA.IA_consume_node import consume_node

HERE = os.path.dirname(os.path.abspath(__file__))


def node_file():
    with open(os.path.join(HERE, 'fixtures/nt6da-source.json')) as json_file:
        return json.loads(json_file.read())


class TestIANode(unittest.TestCase):

    @responses.activate
    @mock.patch('IA.IA_consume_node.os.mkdir')
    def test_node_dump(self, mock_mkdir):
        responses.add(
            responses.Response(
                responses.GET,
                f'{settings.OSF_API_URL}v2/guids/nt6da',
                json=node_file(),
            )
        )

        with mock.patch('builtins.open', mock.mock_open()) as m:
            consume_node('nt6da', 'tests', 'asdfasdfasdgfasg')
            m.assert_called_with(os.path.join(HERE, 'nt6da/node/nt6da.json'), 'w')
            mock_mkdir.assert_called_with(os.path.join(HERE, 'nt6da/node'))
            m.return_value.write.assert_called()

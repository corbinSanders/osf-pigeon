import os
import json
import pytest
import mock
import responses
import tempfile
from pigeon import (
    get_and_write_file_data_to_temp,
    get_and_write_json_to_temp,
    bag_and_tag,
    create_zip_data,
    upload,
    update_metadata
)

HERE = os.path.dirname(os.path.abspath(__file__))

import settings

def node_metadata():
    with open(os.path.join(HERE, 'fixtures/metadata-resp-with-embeds.json'), 'r') as fp:
        return json.loads(fp.read())


def datacite_xml():
    with open(os.path.join(HERE, 'fixtures/datacite-metadata.xml'), 'r') as fp:
        return fp.read()


class TestGetAndWriteFileDataToTemp:

    @pytest.fixture
    def guid(self):
        return 'guid0'

    @pytest.fixture
    def zip_name(self):
        return 'archived_files.zip'

    @pytest.fixture
    def zip_data(self):
        return b'Brian Dawkins on game day'

    def test_get_and_write_file_data_to_temp(self, mock_waterbutler, guid, zip_name, zip_data):
        with tempfile.TemporaryDirectory() as temp_dir:
            get_and_write_file_data_to_temp(
                f'{settings.OSF_FILES_URL}v1/resources/{guid}/providers/osfstorage/?zip=',
                temp_dir,
                zip_name
            )
            assert len(os.listdir(temp_dir)) == 1
            assert os.listdir(temp_dir)[0] == zip_name
            assert open(os.path.join(temp_dir, zip_name), 'rb').read() == zip_data


class TestGetAndWriteJSONToTemp:

    @pytest.fixture
    def guid(self):
        return 'guid0'

    @pytest.fixture
    def json_data(self):
        with open(os.path.join(HERE, 'fixtures/metadata-resp-with-embeds.json'), 'r') as fp:
            return fp.read()

    def test_get_and_write_file_data_to_temp_multipage(self, mock_osf_api, guid, json_data):
        mock_osf_api.add(
            responses.GET,
            f'{settings.OSF_API_URL}v2/guids/{guid}',
            status=200,
            body=json_data
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            get_and_write_json_to_temp(
                f'{settings.OSF_API_URL}v2/guids/{guid}',
                temp_dir,
                'info.json'
            )
            assert len(os.listdir(temp_dir)) == 1
            assert os.listdir(temp_dir)[0] == 'info.json'


class TestBagAndTag:

    @pytest.fixture
    def guid(self):
        return 'guid0'

    def test_bag_and_tag(self, mock_datacite, guid):
        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch('pigeon.bagit.Bag') as mock_bag:
                bag_and_tag(temp_dir, guid)
                mock_bag.assert_called_with(temp_dir)


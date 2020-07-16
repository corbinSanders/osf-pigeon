import pytest
import responses
import settings


@pytest.fixture
def mock_waterbutler(guid, zip_data):
    with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
        rsps.add(
            responses.GET,
            f'{settings.OSF_FILES_URL}v1/resources/{guid}/providers/osfstorage/?zip=',
            status=200,
            body=zip_data
        )
        yield rsps


@pytest.fixture
def mock_osf_api(guid):
    with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
        yield rsps


@pytest.fixture
def mock_datacite(guid):
    with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
        rsps.add(responses.GET, f'https://mds.test.datacite.org/metadata/10.70102/FK2osf.io/{guid}', status=200)
        yield rsps



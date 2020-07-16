import re
import time
import json
import os
from io import BytesIO
from datetime import datetime
import argparse
import internetarchive

import tempfile
import asyncio

from utils import (
    get_paginated_data,
    get_with_retry,
    build_doi,
    get_datacite_metadata
)
import settings
import zipfile
import bagit


def get_and_write_file_data_to_temp(url, temp_dir, dir_name):
    response = get_with_retry(url)
    with open(os.path.join(temp_dir, dir_name), 'wb') as fp:
        fp.write(response.content)


def get_and_write_json_to_temp(url, temp_dir, filename):
    pages = asyncio.run(get_paginated_data(url))
    with open(os.path.join(temp_dir, filename), 'w') as fp:
        fp.write(json.dumps(pages))


def bag_and_tag(temp_dir, guid):
    doi = build_doi(guid)
    xml_metadata = get_datacite_metadata(doi)

    with open(os.path.join(temp_dir, 'datacite.xml'), 'w') as fp:
        fp.write(xml_metadata)

    bagit.make_bag(temp_dir)
    bag = bagit.Bag(temp_dir)
    assert bag.is_valid()


def create_zip_data(temp_dir):
    zip_data = BytesIO()
    zip_data.name = 'bag.zip'
    with zipfile.ZipFile(zip_data, "w") as zip_file:
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_name = re.sub(f"^{temp_dir}", "", file_path)
                zip_file.write(file_path, arcname=file_name)
    zip_data.seek(0)
    return zip_data


def upload(guid, zip_data):
    session = internetarchive.get_session(
        config={
            's3': {
                'access': settings.IA_ACCESS_KEY,
                'secret': settings.IA_SECRET_KEY
            }
        }
    )
    ia_item = session.get_item(guid)

    ia_item.upload(
        zip_data,
        headers={'x-archive-meta01-collection': settings.OSF_COLLECTION_NAME},
        access_key=settings.IA_ACCESS_KEY,
        secret_key=settings.IA_SECRET_KEY,
    )

    return ia_item


def get_metadata(temp_dir, filename):
    with open(os.path.join(temp_dir, 'data', filename), 'r') as f:
        node_json = json.loads(f.read())['data']['attributes']

    date_string = node_json['date_created']
    date_string = date_string.partition('.')[0]
    date_time = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")

    metadata = dict(
        title=node_json['title'],
        description=node_json['description'],
        date=date_time.strftime("%Y-%m-%d"),
        contributor='Center for Open Science',
    )

    article_doi = node_json['article_doi']
    metadata['external-identifier'] = f'urn:doi:{article_doi}'
    return metadata


def modify_metadata_with_retry(ia_item, metadata, retries=2, sleep_time=10):
    try:
        ia_item.modify_metadata(metadata)
    except internetarchive.exceptions.ItemLocateError as e:
        if 'Item cannot be located because it is dark' in str(e) and retries > 0:
            time.sleep(sleep_time)
            retries -= 1
            modify_metadata_with_retry(ia_item, metadata, retries, sleep_time)
        else:
            raise e


def pigeon(guid):
    with tempfile.TemporaryDirectory() as temp_dir:
        get_and_write_file_data_to_temp(
            f'{settings.OSF_FILES_URL}v1/resources/{guid}/providers/osfstorage/?zip=',
            temp_dir,
            'archived_files.zip'
        )
        get_and_write_json_to_temp(
            f'{settings.OSF_API_URL}v2/registrations/{guid}/wikis/',
            temp_dir,
            'wikis.json'
        )
        get_and_write_json_to_temp(
            f'{settings.OSF_API_URL}v2/registrations/{guid}/logs/',
            temp_dir,
            'logs.json'
        )
        get_and_write_json_to_temp(
            f'{settings.OSF_API_URL}v2/guids/{guid}',
            temp_dir,
            'registraton.json'
        )

        bag_and_tag(temp_dir, guid)

        zip_data = create_zip_data(temp_dir)
        ia_item = upload(guid, zip_data)

        metadata = get_metadata(temp_dir, 'registraton.json')
        modify_metadata_with_retry(ia_item, metadata)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-g',
        '--guid',
        help='This is the GUID of the target node on the OSF',
        required=True
    )
    args = parser.parse_args()
    guid = args.guid
    pigeon(guid)
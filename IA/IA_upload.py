import os
import argparse
import json
import internetarchive
from datetime import datetime
from xml.dom import minidom
import asyncio
from settings import (
    OSF_COLLECTION_NAME,
    IA_ACCESS_KEY,
    IA_SECRET_KEY,
)

HERE = os.path.dirname(os.path.abspath(__file__))


def gather_and_upload(item_name: str, parent: str, guid: str):
    '''
    This script uploads everything in a directory to Internet Archive.
    '''

    session_data = {'s3': {'access': IA_ACCESS_KEY, 'secret': IA_SECRET_KEY}}
    session = internetarchive.get_session(config=session_data)

    ia_item = session.get_item(item_name)

    directory_path = os.path.join(HERE, parent)

    '''
    The slash is added to add just directory contents, not the directory itself.
    See: https://archive.org/services/docs/api/internetarchive/quickstart.html
    '''

    ia_upload(ia_item, directory_path + '/', session)
    upload_metadata(ia_item, guid, parent, session)


def upload_metadata(ia_item: internetarchive.item,
                    guid: str, directory: str, session: internetarchive.session):

    node_path = os.path.join(HERE, directory, 'data', guid, 'node', f'{guid}.json')
    with open(node_path, 'r') as f:
        node_json = json.loads(f.read())['attributes']

    xml_path = os.path.join(HERE, directory, 'data', 'datacite.xml')
    xml_data = minidom.parse(xml_path)
    creators = xml_data.getElementsByTagName('creatorName')

    date_string = node_json['date_created']
    date_string = date_string.partition('.')[0]
    date_time = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")

    metadata_dict = dict(
        title=node_json['title'],
        description=node_json['description'],
        creator=creators[0].firstChild.data,
        date=date_time.strftime("%Y-%m-%d"),
        subjects=', '.join(node_json['subjects']),
        contributor='Center for Open Science',
    )

    article_doi = node_json['article_doi']
    metadata_dict['external-identifier'] = f'urn:doi:{article_doi}'
    ia_item.modify_metadata(metadata_dict)
    print("Metadata updated")


def ia_upload(ia_item: internetarchive.item, filename: str, session: internetarchive.session):
    headers = {
        'x-archive-meta01-collection': OSF_COLLECTION_NAME,
    }
    print(filename)
    ia_item.upload(
        filename,
        headers=headers,
        access_key=IA_ACCESS_KEY,
        secret_key=IA_SECRET_KEY,
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i',
        '--item',
        help='The name of the item you want to dump in.',
        required=True,
    )
    parser.add_argument(
        '-s',
        '--source',
        help='The name of the folder you want to dump.',
        required=True,
    )
    args = parser.parse_args()
    item = args.item
    source = args.source

    asyncio.run(gather_and_upload(item, source))

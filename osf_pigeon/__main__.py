import argparse
from osf_pigeon.pigeon import main


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
    main(guid)

import argparse

def parse_cli():
    parser = argparse.ArgumentParser()
    
    # MongoDB arguments
    parser.add_argument('--mongo-migration', default='false', help='Use MongoDB')
    parser.add_argument('--mongo-host', default='localhost', help='MongoDB host.')
    parser.add_argument('--mongo-port', type=int, default=27017, help='MongoDB port.')
    parser.add_argument('--mongo-db', default='relation', help='MongoDB database.')
    parser.add_argument('--mongo-dummy-collection', default='dummy', help='MongoDB collection.')
    parser.add_argument('--mongo-relation-collection', default='relation', help='MongoDB collection.')
    parser.add_argument('--mongo-user', default='', help='MongoDB User.')
    parser.add_argument('--mongo-password', default='', help='MongoDB Password.')


    return parser.parse_args()

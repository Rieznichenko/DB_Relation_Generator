import os
import io
import requests
import zipfile
import tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from relationship import Relationship
from config import parse_cli
import pandas as pd
import json
from pymongo import MongoClient, UpdateOne
from config import parse_cli
from collections import deque
import json
from bson.objectid import ObjectId
#dreamseekers-5f17z1gxnh
def main():
    args = parse_cli()
    relationship = Relationship(args=args)
    
    # Generate & Store Dummy Data
    # dummy_data = relationship.generate_random_data(10000)
    # relationship.save_to_json(dummy_data)
    # relationship.insert_to_mongodb(10000)
    
    relationship.dfs75()

if __name__ == "__main__":
    main()

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


#dreamseekers-5f17z1gxnh
def main():
    args = parse_cli()
    relationship = Relationship(args=args)
    
    # Generate & Store Dummy Data
    # dummy_data = relationship.generate_random_data(10000)
    # relationship.save_to_json(dummy_data)
    # relationship.insert_to_mongodb(100000)
    relationship.generate_relation()
    print("Finished processing all files.")

if __name__ == "__main__":
    main()

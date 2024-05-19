import logging
import pandas as pd
from pymongo import MongoClient
from config import parse_cli
import json
import uuid
from bson.objectid import ObjectId
import copy

class Relationship:
    def __init__(self, args):
        self.chunk_size = 10000
        self.count = 0
        self.mongo_uri = f"mongodb://{args.mongo_host}:{args.mongo_port}"
        self.mongo_db = args.mongo_db
        self.mongo_dummy_collection = args.mongo_dummy_collection
        self.mongo_relation_collection = args.mongo_relation_collection
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.collection = self.db[self.mongo_dummy_collection]
        self.ip_address_collection = self.db["ip_address_link"]
        self.domain_collection = self.db["domain_link"]
        self.sha256_collection = self.db["sha256_link"]
        self.ip_address_collection.create_index("value")
        self.domain_collection.create_index("value")
        self.sha256_collection.create_index("value")
        self.data = self.load_data()
        self.link = {i: {} for i in range(3)}
        self.vis = {}
        self.mp = {}
        self.init_data()

    def load_data(self):
        print("Starting load_data method")
        data = []
        i = 0
        while True:
            try:
                chunk = list(self.collection.find({}, {"_id": 1, "ip_address": 1, "domain": 1, "sha256": 1}).skip(i).limit(self.chunk_size))
                if not chunk:  # no more documents to fetch
                    break
                data.extend(self.convert_objectid(chunk))
                print(f"Loaded chunk {i // self.chunk_size + 1}")
                i += self.chunk_size
            except Exception as e:
                logging.error(f"Error loading data: {e}")
                break
        print("Finished load_data method")
        return data

    def init_data(self):
        print("Starting init_data method")
        index = 0
        for d in self.data:
            index = index + 1
            print(f"Processing data: {index}")
            self.mp[d['_id']] = d
            self.link[0][d['_id']] = []
            self.link[1][d['_id']] = []
            self.link[2][d['_id']] = []
            self.vis[d['_id']] = 0

            self.link[0][d['_id']] = self.get_related_ids(d['ip_address'], self.ip_address_collection, 'ip_address', d['_id'])
            self.link[1][d['_id']] = self.get_related_ids(d['domain'], self.domain_collection, 'domain', d['_id'])
            self.link[2][d['_id']] = self.get_related_ids(d['sha256'], self.sha256_collection, 'sha256', d['_id'])
        print("Finished init_data method")

    def get_related_ids(self, field_value, collection, field, _id):
        if field_value is not None:
            # Check if a document with the value field_value exists in the collection
            doc = collection.find_one({"value": field_value})
            if doc is None:
                # If not, find all related _id's
                related_docs = self.convert_objectid(list(self.collection.find({field: field_value}, {"_id": 1})))
                # Create a document with the field_value as the value and the related _id's as the elements, and add it to the collection
                collection.insert_one({"value": field_value, "elements": [doc['_id'] for doc in related_docs if doc['_id'] != _id]})
            elif len(doc["elements"]) == 0:
                # If there's a unique relation return empty array
                return []
            else:
                # If a document with the value field_value exists, fetch the related _id's from the elements of the document
                related_docs = [related_id for related_id in doc["elements"] if related_id != _id]
            return related_docs
        return []


    def generate_relation(self, idx, state):
        self.count = self.count + 1
        if self.count % 10000 == 0:
            print(f'{self.count} relations have been generated.')
        ret = copy.deepcopy(self.mp[idx])

        for i in range(0, 3):
            if state == i:
                continue

            array = []

            for des in self.link[i][idx]:
                if self.vis[des] == 0:  # Change this line
                    self.vis[des] = 1   # And this line
                    result = self.generate_relation(des, i)  # And this line
                    array.append(result)

            if len(array) > 0:
                if i == 0:
                    ret["ip_relation"] = array
                elif i == 1:  
                    ret["domain_relation"] = array
                else:
                    ret["file_relation"] = array

        return ret

    def dfs75(self):
        self.result = []
        print(self.data)
        for node in self.data:
            self.vis[node['_id']] = 1
            self.result.append(self.generate_relation(node['_id'], -1))
           # self.vis[node['_id']] = 0

        with open('relations.json', 'w') as f:
            json.dump(self.result, f, indent=4)

        print("Finished processing all files.")

    def convert_objectid(self,docs):
        for doc in docs:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
        return docs
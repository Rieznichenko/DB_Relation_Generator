import pandas as pd
from pymongo import MongoClient, UpdateOne
from config import parse_cli
from collections import deque
import json
import random
import uuid
import copy
from bson.objectid import ObjectId
import sys

sys.setrecursionlimit(200000)
class Relationship:
    def __init__(self, args):
        self.chunk_size = 1
        self.count = 0
        # MongoDB configuration
        self.mongo_uri = f"mongodb://{args.mongo_host}:{args.mongo_port}"
        self.mongo_db = args.mongo_db
        self.mongo_dummy_collection = args.mongo_dummy_collection
        self.mongo_relation_collection = args.mongo_relation_collection

        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        collection = db[self.mongo_dummy_collection]
        self.data = self.convert_objectid(list(collection.find()))

        self.link = {}
        self.vis = {}
        self.mp = {}
        for i in range(0, 3):
            self.link[i] = {}

        for d in self.data:
            self.mp[d['id']] = d
            self.link[0][d['id']] = []
            self.link[1][d['id']] = []
            self.link[2][d['id']] = []

            if d["ip_address"] is not None:
                self.link[0][d['id']] = self.convert_objectid(list(collection.find({"ip_address": d["ip_address"]})))
            if d["domain"] is not None:
                self.link[1][d['id']] = self.convert_objectid(list(collection.find({"domain_name": d["domain"]})))
            if d["sha256"] is not None:
                self.link[2][d['id']] = self.convert_objectid(list(collection.find({"file_hash": d["sha256"]})))

            self.vis[d['id']] = 0
        print("Init data successfully")

    def generate_random_data(self, count):
        data = []
        for _ in range(count):
            data.append({
                "id": random.randint(1, 100000),
                "domain_name": f"{uuid.uuid4()}.com",
                "ip_address": ".".join(map(str, (random.randint(0, 255) for _ in range(4)))),
                "file_hash": str(uuid.uuid4()),
                "email": f"{uuid.uuid4()}@example.com"
            })
        return data

    def save_to_json(self, data):
        with open('domain.json', 'w') as f:
            json.dump([d["domain_name"] for d in data], f)
        with open('ip.json', 'w') as f:
            json.dump([d["ip_address"] for d in data], f)
        with open('file.json', 'w') as f:
            json.dump([d["file_hash"] for d in data], f)
        with open('email.json', 'w') as f:
            json.dump([d["email"] for d in data], f)

    def load_from_json(self):
        with open('domain.json', 'r') as f:
            domain = json.load(f)
        with open('ip.json', 'r') as f:
            ip = json.load(f)
        with open('file.json', 'r') as f:
            file = json.load(f)
        with open('email.json', 'r') as f:
            email = json.load(f)

        return domain, ip, file, email

    def insert_to_mongodb(self, count):
        domain, ip, file, email = self.load_from_json()

        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        collection = db[self.mongo_dummy_collection]

        # Create an index on id for faster lookups and updates
        collection.create_index("id")
        collection.create_index("domain_name")
        collection.create_index("ip_address")
        collection.create_index("file_hash")

        for _ in range(count // self.chunk_size):
            data = []
            for _ in range(self.chunk_size):
                data.append({
                    "id": random.randint(1, 100000),
                    "domain_name": random.choice(domain),
                    "ip_address": random.choice(ip),
                    "file_hash": random.choice(file),
                    "email": random.choice(email)
                })

            # Prepare operations for bulk_write
            operations = [UpdateOne({"id": d["id"]}, {"$set": d}, upsert=True) for d in data]

            # Perform bulk_write
            collection.bulk_write(operations)

    def generate_relation(self, idx, state):
        #if idx not in self.link[state]:  # Base case
        #    return
        self.count = self.count + 1
        if count % 10000 == 0:
            print(f'{count} relations have been generated.')
        ret = copy.deepcopy(self.mp[idx])

        for i in range(0, 3):
            if state == i:
                continue

            array = []

            for des in self.link[i][idx]:
                if self.vis[des['id']] == 0:
                    self.vis[des['id']] = 1
                    result = self.generate_relation(des['id'], i)
                  #  self.vis[des['id']] = 0

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
            self.vis[node['id']] = 1
            self.result.append(self.generate_relation(node['id'], -1))
           # self.vis[node['id']] = 0

        with open('relations.json', 'w') as f:
            json.dump(self.result, f, indent=4)

        print("Finished processing all files.")

    def convert_objectid(self,docs):
        for doc in docs:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
        return docs
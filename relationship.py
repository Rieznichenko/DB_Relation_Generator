import pandas as pd
from pymongo import MongoClient, UpdateOne
from config import parse_cli
from collections import deque
import json
import random
import uuid

class Relationship:
    def __init__(self, args):
        self.chunk_size = 10000

        # MongoDB configuration
        self.mongo_uri = f"mongodb://{args.mongo_host}:{args.mongo_port}"
        self.mongo_db = args.mongo_db
        self.mongo_dummy_collection = args.mongo_dummy_collection
        self.mongo_relation_collection = args.mongo_relation_collection

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

    def generate_relation(self, parent_ip=None, parent_domain=None, parent_file=None):
        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        collection = db[self.mongo_dummy_collection]
        
        # read data from MongoDB
        data = list(collection.find())
        
        for d in data:
            if 'ip_address' in d and d['ip_address'] != parent_ip:
                ip_relation = collection.find({"ip_address": d["ip_address"]})
                d["ip_relation"] = [i for i in ip_relation if i["id"] != d["id"]]
                
            if 'domain_name' in d and d['domain_name'] != parent_domain:
                domain_relation = collection.find({"domain_name": d["domain_name"]})
                d["domain_relation"] = self.generate_relation([i for i in domain_relation if i["id"] != d["id"]])
                
            if 'file_hash' in d and d['file_hash'] != parent_file:
                file_relation = collection.find({"file_hash": d["file_hash"]})
                d["file_relation"] = self.generate_relation([i for i in file_relation if i["id"] != d["id"]])

        # save the relation data to JSON file
        with open('relations.json', 'w') as f:
            json.dump(data, f)

        # insert the relation data to MongoDB
        relation_collection = db[self.mongo_relation_collection]
        operations = [UpdateOne({"id": d["id"]}, {"$set": d}, upsert=True) for d in data]
        relation_collection.bulk_write(operations)

        return data
import logging
import pandas as pd
from pymongo import MongoClient
from config import parse_cli
import json
import uuid
from bson.objectid import ObjectId
import copy
import sys
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

class Relationship:
    def __init__(self, args):
        self.chunk_size = 1000000
        self.process_size = 1000000
        self.count = 0
        self.mongo_uri = f"mongodb://{args.mongo_host}:{args.mongo_port}"
        self.mongo_db = args.mongo_db
        self.mongo_dummy_collection = args.mongo_dummy_collection
        self.mongo_relation_collection = args.mongo_relation_collection
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.collection = self.db[self.mongo_dummy_collection]
        self.relation_collection = self.db["relations"]
        # set index
        self.relation_collection.create_index("id")
        self.relation_collection.create_index("ip_address")
        self.relation_collection.create_index("domain")
        self.relation_collection.create_index("sha256")
        self.relation_collection.create_index("pa_id")
        self.relation_collection.create_index("relation_type")
        self.load_data()

        self.vis = []
        self.state = []
        self.mp_uid = []
        self.uid = []
        self.mp_id = {}
        self.link = []
        self.data_idx = []
        self.pa = []
        self.processed_count = 0
        self.print_lock = Lock()

        for i in range(3):
            self.mp_uid.append({})
            self.uid.append(0)
            self.link.append([])

        self.init_data()

    def load_data(self):
        print("Starting load_data method")
        while True:
            try:
                chunk = list(self.collection.find({}, {"_id": 1, "ip_address": 1, "domain": 1, "sha256": 1}).skip(self.count).limit(self.chunk_size))
                if not chunk:  # no more documents to fetch
                    break
                for doc in self.convert_objectid(chunk):
                    yield doc
                print(f"Loaded chunk {self.count // self.chunk_size + 1} -> {len(chunk)}")
                self.count += len(chunk)
            except Exception as e:
                logging.error(f"Error loading data: {e}")
                break
        print("Finished load_data method")


    def get_uid(self, idx, str):

        if str == None:
            return -1
        ret = self.mp_uid[idx].get(str)

        if ret == None:
            self.mp_uid[idx][str] = self.uid[idx]
            ret = self.uid[idx]
            self.uid[idx] = self.uid[idx] + 1
            self.link[idx].append([])

        return ret
    
    def init_data(self):
        print("Starting init_data method")
        index = 0
        for d in self.load_data():
            self.mp_id[d['_id']] = index
            id1 = -1
            id2 = -1
            id3 = -1
            if 'ip_address' in d and d['ip_address']:
                id1 = self.get_uid(0, d['ip_address'])
            if 'domain' in d and d['domain']:
                id2 = self.get_uid(1, d['domain'])
            if 'sha256' in d and d['sha256']:
                id3 = self.get_uid(2, d['sha256'])

            if id1 != -1:
                self.link[0][id1].append(index)
            if id2 != -1:
                self.link[1][id2].append(index)
            if id3 != -1:
                self.link[2][id3].append(index)

            self.vis.append(0)
            self.state.append(-1)
            self.pa.append(index)
            self.data_idx.append([id1, id2, id3])

            index = index + 1
        self.idx = index
        print("Finished init_data method")

    def convert_objectid(self,docs):
        for doc in docs:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
        return docs
    
    def prepare_insert_document(self, data, pa_id, relation_type):
        return {
            "id": data['_id'],
            "ip_address": data['ip_address'],
            "domain": data['domain'],
            "sha256": data['sha256'],
            "pa_id": pa_id,
            "relation_type": relation_type
        }

    def insert(self, data):
        # insert data to feeds collection
        data = {
            "id": data['_id'],
            "ip_address": data['ip_address'],
            "domain": data['domain'],
            "sha256": data['sha256']
        }
        ip_doc = self.relation_collection.find_one({'ip_address': data['ip_address']})
        if ip_doc is None or data['ip_address'] is None:
            domain_doc = self.relation_collection.find_one({'domain': data['domain']})
            if domain_doc is None or data['domain'] is None:
                sha256_doc = self.relation_collection.find_one({'sha256': data['sha256']})
                if sha256_doc is None or data['sha256'] is None:
                    data['pa_id'] = None
                    data['relation_type'] = -1
                else:
                    data['pa_id'] = sha256_doc['id']
                    data['relation_type'] = 2
            else:
                data['pa_id'] = domain_doc['id']
                data['relation_type'] = 1
        else:
            data['pa_id'] = ip_doc['id']
            data['relation_type'] = 0
        self.relation_collection.insert_one(data)
    
    def update(self, data):
        # update data to feeds collection
        self.collection.update_one({'_id': data['_id']}, {"$set": data}, upsert=False)
        self.dothis()

    def delete(self, data):
        # remove data from feeds collection
        self.collection.delete_one({'_id': data['_id']})
        self.dothis()
    
    def process_index(self, i):
        data_list = []
        if self.vis[i] == 1:
            return

        self.islands += 1
        self.vis[i] = 1
        self.processed_count += 1
        if self.processed_count % self.process_size == 0:
            with self.print_lock:
                print(str(self.processed_count) + " data processed")
        # get data from db collection
        db_data = self.collection.find_one({}, {"_id": 1, "ip_address": 1, "domain": 1, "sha256": 1}).skip(i)
        data = self.convert_objectid(db_data)
        data_list.append(self.prepare_insert_document(data, None, -1))
        queue = []
        queue.append([i, -1, 0])

        while len(queue) > 0:
            head = queue[0]
            queue.pop(0)
            dp = head[2]

            if dp >= 2:
                continue

            pa_id = head[0]
            pa_state = head[1]

            for j in range(0, 3):
                if j == pa_state:
                    continue
                id = self.data_idx[pa_id][j]

                if id == -1:
                    continue
                for adj in self.link[j][id]:
                    if self.vis[adj] == 1:
                        continue
                    self.vis[adj] = 1
                    self.processed_count += 1
                    if self.processed_count % self.process_size == 0:
                        with self.print_lock:
                            print(str(self.processed_count) + " data processed")
                    self.state[adj] = j
                    self.pa[adj] = pa_id
                    # get data from db collection
                    db_data = self.collection.find_one({}, {"_id": 1, "ip_address": 1, "domain": 1, "sha256": 1}).skip(adj)
                    data = self.convert_objectid(db_data)
                    db_data1 = self.collection.find_one({}, {"_id": 1, "ip_address": 1, "domain": 1, "sha256": 1}).skip(pa_id)
                    data1 = self.convert_objectid(db_data1)
                    data_list.append(self.prepare_insert_document(data, data1['_id'], j))
                    queue.append([adj, j, dp + 1])
        self.relation_collection.insert_many(data_list)
        
    def dothis(self):
        self.starttime = datetime.datetime.now()
        self.relation_collection.delete_many({})
        self.result = []


        self.islands = 0

        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.map(self.process_index, range(self.idx))
        
        print(str(self.processed_count) + " data processed")
        print(str(self.islands) + " counts connected components")

        self.endtime = datetime.datetime.now()

        print("time: " + str(self.endtime - self.starttime))

        print("Finished processing all files.")
#!/usr/bin/python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
import certifi
import os


class MongoServer(object):
    def __init__(
        self,
        host="localhost",
        port="27017",
        password="toor",
        user="user"
    ):
        self.user = os.environ["MONGODB_USER"] if 'MONGODB_USER' in os.environ else user
        self.password = os.environ["MONGODB_PASSWORD"] if 'MONGODB_PASSWORD' in os.environ else password
        self.host = os.environ["MONGODB_HOST"] if 'MONGODB_HOST' in os.environ else host
        self.port = os.environ["MONGODB_PORT"] if 'MONGODB_PORT' in os.environ else port
        self.db_name = None
        self.collection= None
        
    
    def init(self):
        self.client = MongoClient(
            'mongodb://{}:{}@{}:{}/?retryWrites=true&w=majority'.format(self.user, self.password, self.host,self.port)
        )
        
    
    def atlas_init(self):
        self.client = MongoClient(
            'mongodb+srv://{}:{}@{}:{}/?retryWrites=true&w=majority'.format(self.user, self.password, self.host,self.port),certifi.where()
        )
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection]


    def choose_db(self, db_name):
        self.db = self.client[db_name]
    
    def choose_collection(self, collection_name):
        self.collection = self.db[collection_name]

    def get_all_datas(self):
        return self.collection.find()


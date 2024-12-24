import os 
import sys
import json
import pandas as pd
import pymongo

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")

import certifi
ca = certifi.where()

# lets build a class that will push data to the database
class NetworkDataExtract():
    def __init__(self):
        pass

    # csv to json convertor
    def csv_to_json(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            # Here i am first conerting data into json format from csv and then loading data via json func and last converting them into list
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise e
        
    # insert data into the database(MONGO DB)
    def insert_data_mongo_db(self,records,database,collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records

            # noe here i am connecting to the mongo db and sending data to the database
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            self.database  = self.mongo_client[self.database]

            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            return (len(self.records))
         
        except Exception as e:
            raise e
        

# lets test the class
if __name__ == "__main__":
    FILE_PATH = "Network_Data\phisingData.csv"
    DATABASE = "HAMZA_DB"
    COLLECTION = "network_data"
    network_class = NetworkDataExtract()
    records = network_class.csv_to_json(file_path=FILE_PATH)
    # print(records)
    no_records = network_class.insert_data_mongo_db(records=records,database=DATABASE,collection=COLLECTION)
    print(f"Total Records Inserted: {no_records}")
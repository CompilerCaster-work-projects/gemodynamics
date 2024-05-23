from typing import Tuple, List
from datetime import datetime
import sys
import os
sys.path.append("./")

import pandas as pd
from pymongo import MongoClient, database


class DB:
    CONNECTION_STRING = "mongodb://m.mihailov:eevah0aiZieTh@mongo-replica.db.ss-dc.distmed.com:27017/?authSource=history&readPreference=secondary"

    def __init__(self, pipeline: dict) -> None:
        self.pipe = pipeline
        self.db_name = "history"
        self.collection_name = "inspections"
        self.client = self.connect()
        self.db = self.get_db()
        self.collection = self.get_collection()

    def connect(self) -> MongoClient:
        client = MongoClient(self.CONNECTION_STRING)
        return client

    def get_db(self) -> database:
        db = self.client[self.db_name]
        return db

    def get_collection(self) -> database:
        col = self.db[self.collection_name]
        return col

    @staticmethod
    def to_csv(cursor):
        df = pd.DataFrame(list(cursor))
        if '_id' in df:
            del df['_id']
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        return df

    def load(self, start_date: datetime, end_date: datetime, org_ids: List[int]) -> Tuple[pd.DataFrame]:
        pipe1 = self.pipe("sheet1", start_date, end_date, org_ids)
        pipe2 = self.pipe("sheet2", start_date, end_date, org_ids)
        
        df1 = self.collection.aggregate(pipe1, allowDiskUse=True)
        df2 = self.collection.aggregate(pipe2, allowDiskUse=True)
        df1 = self.to_csv(df1)
        df2 = self.to_csv(df2)
        
        return df1, df2

    def close_client(self):
        self.client.close()

    def save(self, name: str, df: pd.DataFrame):
        os.makedirs(f"tmp/{name}", exist_ok=True)
        df.to_csv(f"tmp/{name}/{name}.csv", index=False)

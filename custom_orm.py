import sqlite3
import json


class CustomORM:
    """მარტივი ORM, რომელიც მხარს უჭერს CRUD ოპერაციებს."""

    def __init__(self, db_name="opensea.db"):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

    def create_table(self):
        """ქმნის ბაზის ცხრილს თუ არ არსებობს"""
        query = """
        CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection TEXT,
            name TEXT,
            description TEXT,
            image_url TEXT,
            owner TEXT,
            twitter_username TEXT,
            contracts TEXT
        )
        """
        self.cursor.execute(query)
        self.conn.commit()

    def insert_data(self, data):
        """ჩაწერს მონაცემებს ცხრილში"""
        query = """
        INSERT INTO collections (collection, name, description, image_url, owner, twitter_username, contracts) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        values = [(d["collection"], d["name"], d["description"], d["image_url"], d["owner"], d["twitter_username"],
                   json.dumps(d["contracts"])) for d in data]

        self.cursor.executemany(query, values)
        self.conn.commit()

    def fetch_all(self):
        """აბრუნებს ყველა ჩანაწერს"""
        self.cursor.execute("SELECT * FROM collections")
        return self.cursor.fetchall()

    def fetch_by_collection(self, collection_name):
        """აბრუნებს კონკრეტულ კოლექციას სახელით"""
        query = "SELECT * FROM collections WHERE collection = ?"
        self.cursor.execute(query, (collection_name,))
        return self.cursor.fetchall()

    def delete_all(self):
        """წაშლის ყველა მონაცემს ცხრილიდან"""
        self.cursor.execute("DELETE FROM collections")
        self.conn.commit()

    def close_connection(self):
        """ხურავს კავშირს ბაზასთან"""
        self.conn.close()
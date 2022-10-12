from DbConnector import DbConnector


class Preprocessor:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['db_group22'].list_collection_names()
        print(collections)

    def drop_and_create_coll(self):
        self.drop_coll(collection_name="User")
        self.drop_coll(collection_name="Activity")
        self.drop_coll(collection_name="Trackpoint")
        self.create_coll(collection_name="User")
        self.create_coll(collection_name="Activity")
        self.create_coll(collection_name="Trackpoint")
        # Check that the table is dropped
        self.show_coll()

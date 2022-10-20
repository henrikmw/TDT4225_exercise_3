from pprint import pprint

import bson.objectid

from DbConnector import DbConnector
from Preprocessor import Preprocessor
from decouple import config
import os

from tqdm.auto import tqdm

import pickle


class CollectionHandler:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.users = {}
        self.activities = []
        self.trackpoints = []

    def prepare_user_dict(self):
        # TODO fjerne id i dictionary objekt senere??
        # Initiate user dictionary {"001": {_id: "001", "has_labels": 0, "activities": []}, "002":...
        for user_id_dict in range(183):
            self.users[str(user_id_dict).zfill(3)] = {"_id": str(user_id_dict).zfill(3), "has_labels": 0, "activities": []}

        # Update has_labels based on labeled_ids.txt
        with open("%s/labeled_ids.txt" % (config("ROOT_PATH")), "r") as labeled_ids_file:
            labeled_ids = labeled_ids_file.readlines()
            labeled_ids_stripped = [id.replace("\n", "") for id in labeled_ids]
            for id in labeled_ids_stripped:
                self.users[id.zfill(3)].update({"has_labels": 1})

        print("Should have label 1")
        print(self.users["010"], "\n")

        #collection = self.db["User"]
        #collection.insert_many()

    def insert_data_locally(self):
        for (root, dir, files) in os.walk(config("ROOT_PATH"), topdown=True):
            if root == '../dataset/dataset' or root == '../dataset/dataset/Data':
                pass
            else:
                if not dir:
                    user_id = root.split("/")[4]
                    print(user_id)
                    # Iterate through all .plt files
                    for file in files:
                        # Get trackpoints in .plt file
                        with open("%s/%s" % (root, file), "r") as trackpoints_file:
                            trackpoints_from_file = trackpoints_file.readlines()[6:]
                            if len(trackpoints_from_file) <= 2500:
                                activity_uuid = bson.objectid.ObjectId()
                                trackpoint_ids = []
                                # Iterate and insert trackpoints
                                for trackpoint in trackpoints_from_file:
                                    trackpoint_uuid = bson.objectid.ObjectId()
                                    trackpoint_ids.append(trackpoint_uuid)
                                    trackpoint_split = trackpoint.split(",")
                                    self.trackpoints.append({
                                        "_id": trackpoint_uuid,
                                        "activity_id": activity_uuid,
                                        "user_id": user_id,
                                        "location": {
                                            "lat": trackpoint_split[0],
                                            "lon": trackpoint_split[1],
                                            "alt": trackpoint_split[3]
                                        },
                                        "date_days": trackpoint_split[5],
                                        "date_time": trackpoint_split[5] + " " + trackpoint_split[6].replace("\n", ""),
                                    })

                                # Find start and end time from trackpoints
                                start_time_trackpoints = trackpoints_from_file[0].split(",")[5].replace("-", "/") + " " + trackpoints_from_file[0].split(",")[6].replace("-", "/").strip()
                                end_time_trackpoints = trackpoints_from_file[-1].split(",")[5].replace("-", "/") + " " + trackpoints_from_file[-1].split(",")[6].replace("-", "/").strip()

                                # Check if user has labeled activities
                                if self.users[str(user_id).zfill(3)]["has_labels"] == 1:
                                    with open("%s/../labels.txt" % root) as labeled_activities_from_file:

                                        # Checking every labeled activity
                                        for labeled_activity_from_file in labeled_activities_from_file.readlines()[1:]:
                                            # Find start and end time from labeled activities
                                            start_time_activity = labeled_activity_from_file.split("\t")[0]
                                            end_time_activity = labeled_activity_from_file.split("\t")[1]

                                            # If match
                                            if start_time_activity == start_time_trackpoints and end_time_activity == end_time_trackpoints:
                                                self.activities.append({
                                                    "_id": activity_uuid,
                                                    "user_id": user_id,
                                                    "transportation_mode": labeled_activity_from_file.split("\t")[2].replace("\n", ""),
                                                    "start_date_time": start_time_activity,
                                                    "end_date_time": end_time_activity,
                                                    "trackpoints": trackpoint_ids
                                                })
                                                self.users[user_id]["activities"].append(activity_uuid)
                                                break
                                else:
                                    self.activities.append({
                                        "_id": activity_uuid,
                                        "user_id": user_id,
                                        "transportation_mode": "NULL",
                                        "start_date_time": start_time_trackpoints,
                                        "end_date_time": end_time_trackpoints,
                                        "trackpoints": trackpoint_ids
                                    })
                                    self.users[user_id]["activities"].append(activity_uuid)

    def insert_data_db(self):
        print("Insert users")
        self.db["User"].insert_many(list(self.users.values()))
        print("Insert activities")
        self.db["Activity"].insert_many(self.activities)
        print("Insert trackpoints")
        self.db["Trackpoint"].insert_many(self.trackpoints)
        print("Finished inserting data into db")

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

def main():
    program = None
    try:
        preprocessor = Preprocessor()
        #preprocessor.drop_and_create_coll()
        collection_handler = CollectionHandler()
        #collection_handler.prepare_user_dict()
        #collection_handler.insert_data_locally()
        #collection_handler.insert_data_db()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

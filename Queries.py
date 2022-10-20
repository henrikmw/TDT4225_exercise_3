from datetime import timezone, datetime

from haversine import haversine
from tabulate import tabulate

from DbConnector import DbConnector


class Question:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def task_one(self):
        print(f"\nTask 1")
        for table in ["User", "Activity", "Trackpoint"]:
            print(f"{table} has {self.db[table].count_documents(filter={})} documents ")
    def task_two(self):
        print(f"\nTask 2")
        for result in self.db["Activity"].aggregate([
            {
                '$group': {
                    '_id': '$user_id',
                    'activityCount': {
                        '$count': {}
                    }
                }
            }, {
                '$group': {
                    '_id': None,
                    'avg_activities': {
                        '$avg': '$activityCount'
                    },

                }
            }
        ]):
            print(f"Average number of activities per user: {result['avg_activities']}")

    def task_three(self):
        print(f"\nTask 3")
        print("List of top 20 user with highest number of activities")
        count = 1
        for user in self.db["User"].aggregate([
            {
                '$project': {
                    '_id': '$_id',
                    'activitiesCount': {
                        '$size': {
                            '$ifNull': [
                                '$activities', []
                            ]
                        }
                    }
                }
            }, {
                '$sort': {
                    'activitiesCount': -1
                }
            }, {
                '$limit': 20
            }
        ]):
            print(f"{count}. User {user['_id']}: {user['activitiesCount']} activities")
            count += 1
    def task_four(self):
        print(f"\nTask 4")
        userlist = []
        for user in self.db["Activity"].aggregate([
            {
                '$match': {
                    'transportation_mode': 'taxi'
                }
            }
        ]):
            if user['user_id'] not in userlist:
                userlist.append(user['user_id'])
        print(f"Users that have taken taxi:")
        for user in userlist:
            print(user)

def main():
    question = Question()

    question.task_one()
    question.task_two()
    question.task_three()
    question.task_four()
    # question.five()
    # question.six()
    # question.seven()
    # question.eight()
    # question.task_nine()
    # question.task_ten()
    # question.eleven()
    # question.twelve()

if __name__ == '__main__':
    main()

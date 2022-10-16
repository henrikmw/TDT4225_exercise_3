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
        for table in ["User", "Activity", "Trackpoint"]:
            print("{table} has {count} documents".format(
                table=table,
                count=self.db[table].count_documents(filter={})))

    def task_two(self):
        for result in self.db["Activity"].aggregate([
            {
                '$group': {
                    '_id': '$user_id',
                    'activity_count': {
                        '$count': {}
                    }
                }
            }, {
                '$group': {
                    '_id': None,
                    'avg_activities': {
                        '$avg': '$activity_count'
                    },

                }
            }
        ]):
            print(f"{'user_id'}:")
            print(f"Average activities: {result['avg_activities']}")

    def task_three(self):
        print("Top 20 users with the highest number of activities:")
        for user in self.db["User"].aggregate([
            {
                '$project': {
                    '_id': '$user_id',
                    'activity_count': {
                        '$size': {
                            '$if_null': [
                                '$activities', []
                            ]
                        }
                    }
                }
            }, {
                '$sort': {
                    'activity_count': -1
                }
            }, {
                '$limit': 20
            }
        ]):
            print(f"User: {user['_id']} Number of activities: {user['activity_count']}")

    def task_four(self):
        print("Users that have taken a taxi")
        for user in self.db["Activity"].aggregate([
            {
                '$match': {
                    'transportation_mode': 'taxi'
                }
            }
        ]):
            print(f"User: {user['user_id']}")


def main():
    question = Question()

    question.task_one()
    # question.task_two()
    # question.task_three()
    # question.task_four()
    # question.five()
    # question.six()
    # question.seven()
    # question.eight()
    # question.nine()
    # question.ten()
    # question.eleven()
    # question.twelve()

if __name__ == '__main__':
    main()

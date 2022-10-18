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
        for table in ["User"]:
            dic = self.db[table].find_one()
            count = 0
            for key, value in dic.items():
                try:
                    if 0 < int(key) < 1000:
                        count += 1
                except:
                    pass # id_ cannot be converted to int...
            print(f"User has {count} documents")

        for table in ["Activity", "Trackpoint"]:
            print("{table} has {count} documents".format(
                table=table,
                count=self.db[table].count_documents(filter={})))

    def task_two(self):
        print(f"\nTask 2")
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
            print(f"Average number of activities per user: {result['avg_activities']}")

    def task_three(self):
        print(f"\nTask 3")
        print("Top 20 users with the highest number of activities:")

        def second(elem):
            return elem[1]

        dic = self.db["User"].find_one()
        top_users = []

        for key, value in dic.items():
            try:
                if 0 < int(key) < 200:
                    top_users.append((key, len(value['activities'])))
            except:
                pass
        top_users.sort(key=second, reverse=True)
        for i, top in enumerate(top_users[0:20]):
            print("{number}. User {user_id}: {activities} activities".format(
                number=i + 1,
                user_id=top[0],
                activities=top[1]
            ))

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
    # question.nine()
    # question.ten()
    # question.eleven()
    # question.twelve()

if __name__ == '__main__':
    main()

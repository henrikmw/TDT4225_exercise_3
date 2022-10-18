from datetime import timezone, datetime

from haversine import haversine
from tabulate import tabulate

from DbConnector import DbConnector


class Question:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def nine(self):
        # Find all users who have invalid activities, and the number of invalid activities per user.
        print("TASK 9 -----------------------------------")

        # invalid_activities = {}
        #
        # for userID in range(0, 183):
        #     user_id = str(userID).zfill(3)
        #
        #     for user_activities in self.db["Activity"].aggregate([
        #         {
        #             '$match': {
        #                 'user_id': user_id
        #             }
        #         }
        #     ]):
        #         print(user_activities)

    def ten(self):
        # Find the users who have tracked an activity in the Forbidden City of Beijing.
        print("TASK 10  -----------------------------------")
        # prøvde $search med wildcard, $geoNear/$near, $match med og uten $regex

        for location in self.db["Trackpoint"].aggregate([
            {
                '$match': {
                    'lat': {'$regex': '/^39.916/*'}
                }
                # 'lat': {'$regex': '39.916*'}
                # 'lon': '116.397*'

            }
        ]):
            print(location)

        #   print(f'User {x} has tracked activity inside the forbidden city of Beijing')

    def eleven(self):
        # Find all users who have registered transportation_mode and their most used transportation_mode.
        print("TASK 11 -----------------------------------")
        results = {}
        for transportation in self.db["Activity"].aggregate([
            {
                '$match': {
                    'transportation_mode': {
                        '$ne': 'NULL'
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'user': '$user_id',
                        'most_used_transportation': '$transportation_mode'
                    },
                    'transportation_count': {
                        '$count': {}
                    }
                }

            }, {
                '$sort': {'transportation_count': -1}
            }
        ]):
            if transportation["_id"]["user"] not in results:
                user_id = transportation["_id"]["user"]
                most_used_transportation = transportation["_id"]["most_used_transportation"]
                results.update({user_id: most_used_transportation})

        sorted_results = dict(sorted(results.items()))
        for item in sorted_results:
            print("User: {}  Most used transportation: {}".format(item, sorted_results[item]))


def main():
    question = Question()

    # question.one()
    # question.two()
    # question.three()
    # question.four()
    # question.five()
    # question.six()
    # question.seven()
    # question.eight()
    # question.nine()
    # question.ten()
    # question.eleven()


if __name__ == '__main__':
    main()
from haversine import haversine
from tabulate import tabulate

from DbConnector import DbConnector
from datetime import datetime


class Question:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def one(self):
        print(f"\nTask 1")
        for table in ["User", "Activity", "Trackpoint"]:
            print(f"{table} has {self.db[table].count_documents(filter={})} documents ")

    def two(self):
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

    def three(self):
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

    def four(self):
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

    def five(self):
        distinct_activities = self.db["Activity"].distinct("transportation_mode")
        for activity in distinct_activities:
            if activity != "NULL":
                result = self.db["Activity"].find(
                    {
                        "transportation_mode": activity
                    }
                )
                print(activity, "has count", result.count())

    def six(self):
        print("\nTASK 6a \n")
        year_with_most_activities = 0
        for result in self.db["Activity"].aggregate([
            {
                '$group': {
                    '_id': {
                        'year': {
                            '$year': {
                              '$dateFromString': {
                                  'dateString': '$start_date_time',
                                  'format': '%Y/%m/%d %H:%M:%S'
                              }
                            }
                        }
                    },
                    'total_cost_year': {
                        '$count': {}
                    }
                }
            }, {
                '$sort': {
                    'total_cost_year': -1
                }
            }, {
                '$limit': 1
            }
        ]):
            year_with_most_activities += result['_id']['year']
            print(f"{result['_id']['year']} had the most activities")

        print("\nTASK 6b \n")
        hours_each_year = {2008: 0, 2009: 0, 2010: 0, 2011: 0, 2012: 0}
        for year in list(hours_each_year.keys()):
            for result in self.db["Activity"].aggregate([
                {
                    '$project': {
                        'user_id': '$user_id',
                        'year': {
                            '$year': {
                                '$dateFromString': {
                                    'dateString': '$start_date_time',
                                    'format': '%Y/%m/%d %H:%M:%S'
                                }
                            }
                        },
                        'duration': {
                            '$dateDiff': {
                                'startDate': {
                                    '$dateFromString': {
                                        'dateString': '$start_date_time',
                                        'format': '%Y/%m/%d %H:%M:%S'
                                    }
                                },
                                'endDate': {
                                    '$dateFromString': {
                                        'dateString': '$end_date_time',
                                        'format': '%Y/%m/%d %H:%M:%S'
                                    }
                                },
                                'unit': 'hour'
                            }
                        }
                    }
                }, {
                    '$match': {
                        'year': year
                    }
                }, {
                    '$group': {
                        '_id': '$user_id',
                        'activities': {
                            '$count': {}
                        },
                        'total_duration': {
                            '$sum': '$duration'
                        }
                    }
                }, {
                    '$sort': {
                        'total_duration': -1
                    }
                }
            ]):
                hours_each_year[year] += result["total_duration"]
        year_with_most_hours = max(hours_each_year, key=hours_each_year.get)
        print('The year with most hours is', year_with_most_hours, "at", hours_each_year[year_with_most_hours], "hours")
        if year_with_most_hours == year_with_most_activities:
            print("This is the same year as the one with the most activities")
        else:
            print("This is not the same year as the one with the most activities")

    def seven(self):
        print("\nTASK 7 \n \n")
        # Gets all the trackpoints for all the activities in the list
        trackpoints = list(self.db["Trackpoint"].find({"user_id": '112'}))

        current_activity = None
        total_distance = 0
        part_distance = 0
        for i in range(len(trackpoints) - 1):
            # Extracts the locations for the current trackpoint and the next
            # to calculate the distance further down
            loc1 = (float(trackpoints[i]["location"]["lat"]), float(trackpoints[i]["location"]["lon"]))
            loc2 = (float(trackpoints[i+1]["location"]["lat"]), float(trackpoints[i+1]["location"]["lon"]))
            # Checks if we for the next iteration should reset the part-data
            next_id = trackpoints[i+1]["activity_id"]
            # If we are not on the same activity anymore, update the current_activity and
            # add the calculated distance to the total
            if(next_id != current_activity):
                current_activity = next_id
                total_distance += part_distance
                part_distance = 0
            # Adds the distance between the trackpoints to the part_distance
            part_distance += haversine(loc1, loc2)

        print("User 112 walked %s km in 2008" % (total_distance))

    def eight(self):
        print("\nTASK 8 \n \n")
        users = {}

        for user_id in range(0, 183):
            formatted_user_id = str(user_id).zfill(3)

            query_result = self.db["Activity"].aggregate([
                {
                    '$match': {
                        'user_id': f'{formatted_user_id}'
                    }
                }, {
                    '$lookup': {
                        'from': 'Trackpoint',
                        'localField': 'trackpoints',
                        'foreignField': '_id',
                        'as': 'trackpoints_embedded'
                    }
                }
            ])

            for activity in query_result:
                user_id = activity['user_id']

                altitude_total = 0
                prev_alt = -999

                for trackpoint in activity["trackpoints_embedded"]:
                    new_alt = float(trackpoint["location"]["alt"])

                    # First iteration
                    if prev_alt == -999:
                        prev_alt = new_alt

                    if new_alt > prev_alt and new_alt != -777:
                        altitude_total += new_alt - prev_alt

                        users[user_id] = altitude_total * 0.3048

                    prev_alt = new_alt
            print(list(users.keys())[-1])

        sortedResult = sorted(users.items(), key=lambda x: x[1], reverse=True)
        print(tabulate(sortedResult[0:20], headers=('User ID', 'Altitude gained')))

    def nine(self):
        invalid_activities = {}

        for userIDQuery in range(0, 183):
            user_id = str(userIDQuery).zfill(3)
            print(f"Now on user {user_id}")

            query = self.db["Activity"].aggregate([
                {
                    '$lookup': {
                        'from': 'User',
                        'localField': 'user_id',
                        'foreignField': '_id',
                        'as': 'user'
                    }
                }, {
                    '$unwind': {
                        'path': '$user'
                    }
                }, {
                    '$match': {
                        'user._id': user_id
                    }
                }, {
                    '$lookup': {
                        'from': 'Trackpoint',
                        'localField': 'trackpoints',
                        'foreignField': '_id',
                        'as': 'trackpoints_embedded'
                    }
                }
            ])

            for activity in query:
                prev_trackpoint = None

                activity_is_invalid = False

                for trackpoint in activity["trackpoints_embedded"]:
                    # Skip comparison if trackpoint is the first one
                    if prev_trackpoint is None:
                        prev_trackpoint = trackpoint
                        continue
                    # Calculate time in minutes between current trackpoint and old trackpoint
                    time_difference = (datetime.strptime(trackpoint["date_time"], "%Y-%m-%d %H:%M:%S") - datetime.strptime(prev_trackpoint["date_time"], "%Y-%m-%d %H:%M:%S")).total_seconds() / 60

                    # If time difference >= 5 stop iteration
                    if time_difference >= 5:
                        activity_is_invalid = True
                        break

                    # Set old trackpoint to current trackpoint
                    prev_trackpoint = trackpoint

                if activity_is_invalid:
                    if activity['user_id'] not in invalid_activities:
                        invalid_activities[activity['user_id']] = 1
                    else:
                        invalid_activities[activity['user_id']] += 1
            print(invalid_activities)

        for i in invalid_activities:
            print(i, "has", invalid_activities[i], "invalid activities")

    def ten(self):
        # Find the users who have tracked an activity in the Forbidden City of Beijing.
        print("TASK 10  -----------------------------------")
        users = []
        for i in range(182):
            user_id = (str(i).rjust(3, "0"))

            trackpoints = list(self.db["Trackpoint"].find(
                {
                    'user_id': user_id
                }
            ))

            for number in range(len(trackpoints)):
                lat = abs(float(trackpoints[number]["location"]["lat"]) - 39.916)
                lon = abs(float(trackpoints[number]["location"]["lon"]) - 116.397)
                user = trackpoints[number]["user_id"]
                if lat < 0.001 and lon < 0.001:
                    if user not in users:
                        users.append(user)
                        print(f'User {user} has tracked activity inside the forbidden city of Beijing')

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

#    question.one()
#    question.two()
#    question.three()
#    question.four()
#    question.five()
#    question.six()
#    question.seven()
#    question.eight()
#    question.nine()
#    question.ten()
#    question.eleven()


if __name__ == '__main__':
    main()
from haversine import haversine
from tabulate import tabulate

from DbConnector import DbConnector


class Question:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

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

def main():
    question = Question()

    question.five()
    question.six()
    question.seven()
    question.eight()


if __name__ == '__main__':
    main()
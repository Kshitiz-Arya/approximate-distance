import boto3
import os

from haversine import haversine
from openrouteservice import Client
from openrouteservice.distance_matrix import distance_matrix
from h3.api import basic_str as h3


def lambda_handler(event, context):
    
    dynamodb = boto3.resource("dynamodb")
    source_location = event["sourcelocation"]
    primary_key = {"Target_Location_ID": f'{event["TargetLocationID"]}'}

    location_data = get_data(
        table_name="Static-location", key=primary_key, dynamodb=dynamodb
    )["Target_Location_Address"]

    target_location = [
        float(loc) for loc in location_data["location"]
    ]  # converting to float for calculation
    approx_location = get_approx_location(source_location)
    travel_distance = get_distance(
        approx_location, target_location, event["ModeofTransport"]
    )

    insert_data = {
        "Distance_Travelled": str(travel_distance),
        "User_Location": str(approx_location),
        "Target_Location_ID": event["TargetLocationID"],
        "User_Location_UUID": event["UserLocationUUID"],
        "Mode_of_Transport": event["ModeofTransport"],
        "User_UUID": event["UserUUID"],
        "Round_Trip_Indicator": event["RoundTripIndicator"],
    }
    return put_data("Users", insert_data, dynamodb)


def get_approx_location(location):
    hex = h3.geo_to_h3(location[0], location[1], resolution=7)
    return h3.h3_to_geo(hex)


def get_distance(source_location, target_location, travel_mode):
    if travel_mode in ["train", "air"]:
        return haversine(source_location, target_location)

    locations = [
        source_location[::-1],
        target_location[::-1],
    ]  # converting locations from (lat, long) to (long, lat), which is required by openroute
    request = {
        "locations": locations,
        "sources": [0],
        "destinations": [1],
        "metrics": ["distance"],
        "units": "km",
    }
    print(locations, source_location, target_location)
    travel_distance = distance_matrix(
        client=Client(key=os.environ["key"]), **request, profile=travel_mode
    )["distances"][0][0]
    return (
        travel_distance
        if travel_distance is not None
        else haversine(source_location, target_location)
    )


def put_data(table_name, items, dynamodb):
    table = dynamodb.Table(table_name)
    response = table.put_item(Item=items)
    return response


def get_data(table_name, key, dynamodb):

    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(Key=key)
    except Exception as e:
        print(e.response["Error"]["Message"])
    else:
        return response["Item"]


if __name__ == "__main__":
    event = {
        "sourcelocation": [28.55436, 77.339408],
        "TargetLocationID": "target-location-id",
        "UserLocationUUID": "user-location-uuid",
        "ModeofTransport": "foot-walking",
        "UserUUID": "user-uuid",
        "RoundTripIndicator": true,
    }

    lambda_handler(event, None)

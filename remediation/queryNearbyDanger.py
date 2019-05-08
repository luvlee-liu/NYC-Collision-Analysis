#!/usr/bin/env python3
import csv
import sys
import json

DANGER_LEVEL=100
WARN_LEVEL=50
ALERT_DISTANCE = 5e-07 #distance of 1~2 street block
def get_distance(point0, point1):
    lat_diff = float(point0[0]) - float(point1[0])
    long_diff = float(point0[1]) - float(point1[1])
    return lat_diff*lat_diff + long_diff*long_diff

def format_loc(value):
    return '{0:.7f}'.format(value)

def map_url(loc):
    lat, long = loc
    return f'https://www.google.com/maps/search/?api=1&query={format_loc(lat)},{format_loc(long)}'

def json_pretty(data):
    return json.dumps(data, indent=4, separators=(',', ': '))

if len(sys.argv) < 3:
    print("./queryNearbyDanger.py <latitude> <longitude>")
    print("- example: ./queryNearbyDanger.py  40.7334832 -73.9867383")
    print("- example: ./queryNearbyDanger.py  40.7339903 -73.9867068")


latitude = float(sys.argv[1])
longitude = float(sys.argv[2])
current_loc = (latitude, longitude)
print("current at " + map_url(current_loc))

nearest_dist = ALERT_DISTANCE
alert_location = {}
number_incident = 0
results = []
with open('incidentsByStreet.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            # print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            incident_loc = (float(row[4]), float(row[5]))

            dist = get_distance(current_loc, incident_loc)
            # if current_loc is near incident_loc
            if dist < ALERT_DISTANCE:
                # find closest incident_loc
                if dist < nearest_dist:
                    nearest_dist = dist
                    number_incident = int(row[3])
                    st0 = row[0]
                    st1 = row[1]
                    casualty_per_incident = float(row[6])
                    alert_location = {
                        "name": st0 + ','+ st1,
                        "distance": dist,
                        "at": map_url(incident_loc),
                        "number_incident": number_incident,
                        "casualty_per_incident": casualty_per_incident
                    }
            line_count += 1

if number_incident > DANGER_LEVEL:
    alert_location["severity"] = "DANGER"
    print(f'DANGER: slow down and be cautious at:\n{json_pretty(alert_location)}')
elif number_incident > WARN_LEVEL:
    print(f'WARN: keep alert at\n{json_pretty(alert_location)}')
    alert_location["severity"] = "WARN"
else:
    print("SAFE: keep eye on the road")


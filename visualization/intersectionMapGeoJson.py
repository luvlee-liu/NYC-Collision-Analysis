#!/usr/bin/env python3

import csv
import json


def write_json(data, file_name):
    with open(file_name, 'w') as out_file:  
        json.dump(data, out_file, indent=4, separators=(',', ': '))

def get_color_by_severity(casualty_per_incident):
    if casualty_per_incident >= 0.2:
        return '#FF1C1C' # red
    elif casualty_per_incident >= 0.1:
        return '#ffea00' # yellow
    return '#66ff00' # green

def get_size_by_incident(incidents):
    if incidents >= 600:
        return 'large'
    elif incidents >= 400:
        return 'medium'
    return 'small'

def generate_geo_json_top_n(input_csv, topN = None):
    if topN is None:
        topN = 10
    with open(input_csv) as csv_file:
        dialect = csv.Sniffer().sniff(csv_file.read(1024))
        csv_file.seek(0)
        reader = csv.reader(csv_file, dialect)
        next(reader) # skip header
        # STREET0,STREET1,sum(TOTAL),count(TOTAL),avg(LATITUDE),avg(LONGITUDE),CASUALTY_PER_INCIDENT
        # Assuming the csv is sorted
        count = 0
        geojsonFeature = []

        for row in reader:

            severity = float(row[6])
            incidents = float(row[3])
            geojsonFeature.append({
                "type": "Feature",
                'geometry': {
                    'type': "Point",
                    'coordinates': [float(row[5]), float(row[4])]
                },
                'properties': {
                    'marker-size': get_size_by_incident(incidents),
                    'marker-symbol': 'car',
                    'marker-color': get_color_by_severity(severity),
                    'title': row[0] +' & ' + row[1],
                    'total casualty': row[2],
                    'total incidents': incidents,
                    'casualty per incident': severity,
                    'google map url': "https://www.google.com/maps/place/" + row[0] + " & " + row[1] + ", New York, NY"
                }
                
            })
            count += 1
            if count > topN:
                break
        return {
            'type': "FeatureCollection",
            'features': geojsonFeature
        }

if __name__ == "__main__":
    filePath = "./data/incidentCross/part-00000"
# filePath = "./data/dangerCross/part-00000"
    data = generate_geo_json_top_n(filePath, 50)
    write_json(data, "./incidentsMap.geojson")

#### NYC collisions analysis visualization

##### Copy spark analysis results to `data` folder
```
mkdir ./data
hdfs dfs -get collision/output/* ./data/
```
Should include following results
```
ls ./data
alcoholByTime/              groupByBoro/                groupByTimeAndContributing/ nonVehicleByContributing/
dangerCross/                groupByContributing/        groupByTimeQuantity/        nonVehiclePercentByBoro/
glareByTime/                groupByStreetQuantity/      incidentCross/
```
##### Hourly total incident numbers grouped by Contributing factor
start python SimpleHttpServer by
```
./runServer.sh
```
Use chrome browser access `localhost:8000`

##### Map of incidents
Run `intersectionMapGeoJson.py` to generate `incidentsMap.geojson`
Copy and paste content of `incidentsMap.geojson` to `http://geojson.io/`

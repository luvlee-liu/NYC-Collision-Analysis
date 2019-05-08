### Instructions:
#### Download data from
```
https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95
```
Put data into `hdfs` folder `./collision/input/`

#### 1. Build application:
```
module load maven/3.5.2
mvn clean package
```

#### 2. ETL
Launch ETL job on the Spark 1.x of cloudera cdh5.15.1:
```
spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 --class edu.nyu.ll3435.ETLMain ./target/spark-nyc-collisions-0.0.1-SNAPSHOT.jar ./collision/input/ ./collision/output/etl
```
or
```
spark-submit --class edu.nyu.ll3435.ETLMain ./target/spark-nyc-collisions-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./collision/input/ ./collision/output/etl
```

the output path may need to use absolute path like `hdfs://<cluster>/user/<netid>/collision/output/etl`

#### 3. Analysis
Launch Analysis job on the Spark 1.x of cloudera cdh5.15.1: 
```
spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 --class edu.nyu.ll3435.AnalysisMain ./target/spark-nyc-collisions-0.0.1-SNAPSHOT.jar  ./collision/output/etl ./collision/output/
```
or
```
spark-submit --class edu.nyu.ll3435.AnalysisMain ./target/spark-nyc-collisions-0.0.1-SNAPSHOT-jar-with-dependencies.jar  ./collision/output/etl ./collision/output/
```
This job will generate multiple analysis outputs in the sub folder of `./collision/output/`, each sub folder have a 
csv file named `part-00000`

#### 4. Visualization

##### Copy spark analysis results to `data` folder
```
cd ./visualization
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

#### 5. Remediation
A script create alert if nearby location have more incidents in NYC

run `./queryNearbyDanger.py <current latitude> <current longitude>`

examples:
```
cd ./remediation
$ ./queryNearbyDanger.py  40.7339903 -73.9867068
current at: https://www.google.com/maps/search/?api=1&query=40.7339903,-73.9867068
SAFE: keep eye on the road

$ ./queryNearbyDanger.py  40.7334832 -73.9867383
current at: https://www.google.com/maps/search/?api=1&query=40.7334832,-73.9867383
DANGER: slow down and be cautious at:
{
    "name": "EAST 14 STREET,3 AVENUE",
    "distance": 2.8911783228359393e-15,
    "at": "https://www.google.com/maps/search/?api=1&query=40.7334832,-73.9867383",
    "number_incident": 281,
    "casualty_per_incident": 0.24199288256227758,
    "severity": "DANGER"
}
```

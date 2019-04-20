### Instructions:
#### Download data from
```
https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95
```
Put data into `hdfs` folder `./collision/input/`

#### 1. Build application:
```
mvn clean package
```

#### 2. ETL
Launch ETL job on the Spark 1.x of cloudera cdh5.15.1:
```
spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 --class edu.nyu.ll3435.ETLMain 
./target/spark-nyc-collisions-0.0.1-SNAPSHOT.jar ./collision/input/ ./collision/output/etl
```
or
```
spark-submit --class edu.nyu.ll3435.ETLMain ./target/spark-nyc-collisions-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
./collision/input/ ./collision/output/etl
```

the output path may need to use absolute path like `hdfs://<cluster>/user/<netid>/collision/output/etl`

#### 3. Analysis
Launch Analysis job on the Spark 1.x of cloudera cdh5.15.1: 
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

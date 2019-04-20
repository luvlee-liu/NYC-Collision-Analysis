package edu.nyu.ll3435

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}
import com.databricks.spark.csv

object ETLMain {

  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())

    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: ETLMain  <path-to-files> <output-path>")
      System.exit(1)
    }

    val jobName = "NYC Collisions ETL"

    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);

    val pathToFiles = arg(0)
    val outputPath = arg(1)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> pathToFiles \"" + pathToFiles + "\"")

    // load raw data
    val df0 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(pathToFiles)

    // drop col
    val columnNames = Seq("TIME", "BOROUGH", "LATITUDE", "LONGITUDE", "ON STREET NAME", "CROSS STREET NAME", "OFF " +
      "STREET NAME", "NUMBER OF PEDESTRIANS INJURED", "NUMBER OF PEDESTRIANS KILLED", "NUMBER OF CYCLIST INJURED",
      "NUMBER OF CYCLIST KILLED", "NUMBER OF MOTORIST INJURED", "NUMBER OF MOTORIST KILLED", "CONTRIBUTING FACTOR VEHICLE 1")
    val df1 = df0.select(columnNames.head, columnNames.tail: _*)

    // clean string column
    def cleanString(col: Column): Column = {
      // remove leading, trailing and multiple spaces
      trim(regexp_replace(col, "\\s+", " "))
    }
    val df2 = df1.withColumn("BOROUGH", cleanString(col("BOROUGH"))).
      withColumn("ON STREET NAME", cleanString(col("ON STREET NAME"))).
      withColumn("CROSS STREET NAME", cleanString(col("CROSS STREET NAME"))).
      withColumn("OFF STREET NAME", cleanString(col("OFF STREET NAME"))).
      withColumn("CONTRIBUTING FACTOR", cleanString(col("CONTRIBUTING FACTOR VEHICLE 1")))

    // compute "INJURED OR KILLED" col
    val df3 = df2.withColumn("PEDESTRIANS", expr("`NUMBER OF PEDESTRIANS KILLED` + `NUMBER OF " +
      "PEDESTRIANS INJURED`")).
      withColumn("CYCLIST", expr("`NUMBER OF CYCLIST KILLED` + `NUMBER OF CYCLIST INJURED`")).
      withColumn("MOTORIST", expr("`NUMBER OF MOTORIST KILLED` + `NUMBER OF MOTORIST INJURED`"))

    // compute "Total Casualty including INJURED and KILLED"
    val df4 = df3.withColumn("TOTAL", expr("`PEDESTRIANS` + `CYCLIST` + `MOTORIST`"))

    // sorting onStreet, crossStreet and offstreet
    // if onStreet and crossStreet are empty, use offstreet as st0, "" as st1
    // if any of onStreet or crossStreet is not empty, use the greater string as st0, less string as st1
    def st0 = udf((on: String, cross: String, off: String) => {
      if (on == "" && cross == "") {
        off.toUpperCase()
      } else if (on >= cross){
        on.toUpperCase
      } else {
        cross.toUpperCase()
      }
    } )

    def st1 = udf((on: String, cross: String, off: String) => {
      if (on == "" && cross == "") {
        ""
      } else if (on < cross) {
        on.toUpperCase()
      } else {
        cross.toUpperCase()
      }
    } )

    val df5 = df4.withColumn("STREET0", st0(col("ON STREET NAME"), col("CROSS STREET NAME"), col("OFF STREET NAME")) ).
      withColumn("STREET1", st1(col("ON STREET NAME"), col("CROSS STREET NAME"), col("OFF STREET NAME")) )

    // drop invalid rows
    val df6 = df5.where("BOROUGH is not null AND BOROUGH != '' AND TOTAL is not null AND STREET0 is not null AND " +
      "STREET0 != '' AND TIME is not null AND " +
      "LATITUDE is not null AND LATITUDE != 0.0 AND " +
      "LONGITUDE is not null AND LONGITUDE != 0.0")

    // uniform CONTRIBUTING FACTOR, correct spelling and to lower case
    val uniformContributing = udf((factor: String) => {
      if (factor.size < 4) {
        "unspecified"
      }
      else if (factor == "Illnes") {
        "illness"
      }
      else {
        factor.toLowerCase()
      }
    })
    val df7 = df6.withColumn("CONTRIBUTING", uniformContributing(col("CONTRIBUTING FACTOR")))

    // align time to hour
    def toHour = udf((time: String) => {
      time.split(":")(0).toInt
    })
    val df8 = df7.withColumn("HOUR", toHour(col("TIME")))

    // select output columns
    val outputColumnNames = Seq("HOUR", "BOROUGH", "LATITUDE", "LONGITUDE", "STREET0", "STREET1", "TOTAL",
      "PEDESTRIANS", "CYCLIST", "MOTORIST",
      "CONTRIBUTING")
    val outputDf = df8.select(outputColumnNames.head, outputColumnNames.tail: _*)

    // save the csv
    outputDf.cache()
    outputDf.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(outputPath)


    // profiling
    println(outputDf.schema)
    //    org.apache.spark.sql.types.StructType = StructType(StructField(HOUR,StringType,true), StructField(BOROUGH,
    // StringType,true), StructField(LATITUDE,DoubleType,true), StructField(LONGITUDE,DoubleType,true), StructField(STREET0,StringType,true), StructField(STREET1,StringType,true), StructField(Total,IntegerType,true), StructField(PEDESTRIANS,IntegerType,true), StructField(CYCLIST,IntegerType,true), StructField(MOTORIST,IntegerType,true), StructField(CONTRIBUTING FACTOR,StringType,true))
    def summary = () => {
      println("Number of Rows: " + outputDf.count())
    }
    def profileNumber = (colName: String, valueType: String) => {
      val result = outputDf.select(min(col(colName)), max(col(colName))).collect()(0)
      println("column: " +  colName + " type: " + valueType + " min: " + result(0) + " max: " + result(1))
    }

    def profileString = (colName: String) => {
      val result = outputDf.select(length(col(colName))).collect()(0)
      println("column: " +  colName + " type: String" + " length: " + result(0))
    }

    summary()
    profileNumber("HOUR", "Int")
    profileString("BOROUGH")
    profileNumber("LATITUDE", "Double")
    profileNumber("LONGITUDE", "Double")
    profileString("STREET0")
    profileString("STREET1")
    profileNumber("TOTAL", "Int")
    profileNumber("PEDESTRIANS", "Int")
    profileNumber("CYCLIST", "Int")
    profileNumber("MOTORIST", "Int")
    profileString("CONTRIBUTING")
  }
}

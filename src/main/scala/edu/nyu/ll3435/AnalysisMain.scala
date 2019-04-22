package edu.nyu.ll3435

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}

object AnalysisMain {

  private var outputPath = "./"

  def add = udf(( a:Double, b:Double) => {
    a + b
  })

  def divide = udf(( a:Double, b:Double) => {
    a / b
  })

  def csvWrite(df: DataFrame, outputPath: String) = {
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(outputPath)
  }

  // describe the severity of the incident
  def casualtyByIncident(df: DataFrame, colName: String) = {
    val df1 = df.withColumn("CASUALTY_PER_INCIDENT", divide(col("sum(" +colName+ ")"),col("count("+colName+")")))
    df1
  }

  def groupByBoro(df: DataFrame) = {
    val df1 = df.select( "BOROUGH", "TOTAL").
      groupBy("BOROUGH").agg(sum("TOTAL"), count("TOTAL"))

    val df2 = casualtyByIncident(df1, "TOTAL").orderBy(desc("CASUALTY_PER_INCIDENT"))

    df2
  }

  def nonVehicleCasuaty(df: DataFrame) = {
    val df1 = df.select( "BOROUGH", "TOTAL", "PEDESTRIANS_CYCLIST").
      groupBy("BOROUGH").agg(sum("PEDESTRIANS_CYCLIST"), sum("TOTAL"))

    val df2 = df1.withColumn("PEDESTRIANS_CYCLIST_IN_TOTAL", divide(col("sum(PEDESTRIANS_CYCLIST)"),col("sum(TOTAL)"))).
      orderBy(desc("PEDESTRIANS_CYCLIST_IN_TOTAL"))
    df2
  }

  def nonVehicleGroupByContributingQuantity(df: DataFrame) = {
    // filter incidents having PEDESTRIANS or CYCLIST casualty
    val nonVehicle = df.where("PEDESTRIANS > 0 OR CYCLIST > 0")
    val df1 = nonVehicle.select( "CONTRIBUTING", "PEDESTRIANS_CYCLIST").groupBy("CONTRIBUTING").agg(sum
    ("PEDESTRIANS_CYCLIST"), count("PEDESTRIANS_CYCLIST"))

    val df2 = casualtyByIncident(df1, "PEDESTRIANS_CYCLIST").orderBy(desc("sum(PEDESTRIANS_CYCLIST)"))

    df2
  }

  def groupByContributingQuantity(df: DataFrame) = {
    val df1 = df.select( "CONTRIBUTING", "TOTAL").groupBy("CONTRIBUTING").agg(sum
    ("TOTAL"), count("TOTAL"))

    val df2 = casualtyByIncident(df1, "TOTAL").orderBy(desc("sum(TOTAL)"))

    df2
  }

  def groupByTimeQuantity(df: DataFrame) = {
    val df1 = df.select( "HOUR", "TOTAL").groupBy("HOUR").agg(sum
    ("TOTAL"), count("TOTAL"))

    val df2 = casualtyByIncident(df1, "TOTAL").orderBy(asc("HOUR"))
    df2
  }

  def groupByTimeAndContributingQuantity(df: DataFrame) = {
    val df1 = df.select( "HOUR", "CONTRIBUTING", "TOTAL").groupBy("HOUR","CONTRIBUTING").agg(sum
    ("TOTAL"), count("TOTAL"))

    val df2 = casualtyByIncident(df1, "TOTAL").orderBy(asc("HOUR"), desc("sum(TOTAL)"))
    df2
  }

  def groupByStreetQuantity(df: DataFrame) = {
    val df1 = df.select("STREET0", "STREET1", "LATITUDE", "LONGITUDE", "TOTAL").groupBy("STREET0", "STREET1").agg(sum
    ("TOTAL"), count("TOTAL"), avg("LATITUDE"), avg("LONGITUDE"))
    val df2 = casualtyByIncident(df1, "TOTAL").orderBy(desc("CASUALTY_PER_INCIDENT"))
    df2
  }

  def filterByStreet(df: DataFrame, st0: String, st1: String) = {
    df.where("STREET0 = '" + st0 + "' AND STREET1 = '"+ st1 + "'")
  }

  def crossContributingAndTime(df: DataFrame, st0: String, st1: String) = {
    val cross = filterByStreet(df, "60 STREET","3 AVENUE")
    groupByContributingQuantity(cross).orderBy(desc("count(TOTAL)")).show(100, false)
    groupByTimeQuantity(cross).show(24, false)
  }

  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())

    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: ETLMain  <path-to-files> <output-path>")
      System.exit(1)
    }

    val jobName = "NYC Collisions Analysis"

    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);

    val pathToFiles = arg(0)
    outputPath = arg(1)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> pathToFiles \"" + pathToFiles + "\"")

    // load ETL results
    val df0 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").
      option("inferSchema", "true").load(pathToFiles)

    val df1 = df0.withColumn("PEDESTRIANS_CYCLIST", add(col("PEDESTRIANS"), col("CYCLIST")))

    val data = df1
    data.cache()

    // Analysis: groupby BOROUGH
    val dfGroupByBoro = groupByBoro(data)
    csvWrite(dfGroupByBoro, outputPath + "/groupByBoro")

    // Analysis: PEDESTRIANS and CYCLIST casualty in total groupby BOROUGH for
    val dfNonVehicleCasuaty = nonVehicleCasuaty(data)
    csvWrite(dfNonVehicleCasuaty, outputPath + "/nonVehiclePercentByBoro")

    // Analysis: total incident group by contributing factor
    val dfGroupByContributingQuantity = groupByContributingQuantity(data)
    csvWrite(dfGroupByContributingQuantity, outputPath + "/groupByContributing")

    // Analysis: incident having pedestrians and cyclists casualty group by contributing factor
    val dfNonVehicleGroupByContributingQuantity = nonVehicleGroupByContributingQuantity(data)
    csvWrite(dfNonVehicleGroupByContributingQuantity, outputPath + "/nonVehicleByContributing")


    // Analysis: groupby time sort by number of incident
    val dfGroupByTimeQuantity = groupByTimeQuantity(data)
    csvWrite(dfGroupByTimeQuantity, outputPath + "/groupByTimeQuantity")

    // Analysis: groupby time and CONTRIBUTING
    val dfGroupByTimeAndContributingQuantity = groupByTimeAndContributingQuantity(data)
    csvWrite(dfGroupByTimeAndContributingQuantity, outputPath + "/groupByTimeAndContributing")

    // glare by hour
    val dfGlare = dfGroupByTimeAndContributingQuantity.where("CONTRIBUTING = 'glare'")
    dfGlare.show(24,false)
    csvWrite(dfGlare, outputPath + "/glareByTime")

    // alcohol involvement
    val dfAlcohol = dfGroupByTimeAndContributingQuantity.where("CONTRIBUTING = 'alcohol involvement'")
    dfAlcohol.show(24,false)
    csvWrite(dfAlcohol, outputPath + "/alcoholByTime")

    // Analysis: total incident group by streets
    val dfGroupByStreetQuantity = groupByStreetQuantity(data)
    csvWrite(dfGroupByStreetQuantity, outputPath + "/groupByStreetQuantity")

    // show top incidents cross
    val dfIncidentCross = dfGroupByStreetQuantity.orderBy(desc("count(TOTAL)"))
    csvWrite(dfIncidentCross, outputPath + "/incidentCross")
    dfIncidentCross.show(50, false) //top50IncidentCross

    // Analysis a cross by time and contributing
    val topIncidentCross = dfIncidentCross.first()
    crossContributingAndTime(data, topIncidentCross.getString(0), topIncidentCross.getString(1))


    // show top severity cross when number of incident more that 100
    val dfDangerCross = dfGroupByStreetQuantity.filter(dfGroupByStreetQuantity("count(TOTAL)") > 100)
    csvWrite(dfDangerCross, outputPath + "/dangerCross")
    dfDangerCross.show(50, false)

    // Analysis a cross by time and contributing
    val topDangerCross = dfDangerCross.first()
    crossContributingAndTime(data, topDangerCross.getString(0), topDangerCross.getString(1))

  }
}

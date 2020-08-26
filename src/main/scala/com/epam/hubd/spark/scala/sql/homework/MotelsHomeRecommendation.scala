package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))
    val sqlContext = new HiveContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
     * Task 1:
     * Read the bid data from the provided file.
     */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)


    /**
     * Task 1:
     * Collect the errors and save the result.
     */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
     * Task 2:
     * Read the exchange rate information.
     * Hint: You will need a mapping between a date/time and rate
     */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
     * Task 3:
     * UserDefinedFunction to convert between date formats.
     * Hint: Check the formats defined in Constants class
     */
    val convertDate: UserDefinedFunction = sqlContext.udf.register("convertDate", getConvertDate(_: String))

    val calculateEURPrice: UserDefinedFunction = sqlContext.udf.register("calculateEURPrice", getCalculateEURPrice(_: String, _: String))

    /**
     * Task 3:
     * Transform the rawBids
     * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
     * - Convert dates to proper format - use formats in Constants util class
     * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
     */
    val bids: DataFrame = getBids(sqlContext: HiveContext, rawBids, exchangeRates)

    /**
     * Task 4:
     * Load motels data.
     * Hint: You will need the motels name for enrichment and you will use the id for join
     */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
     * Task5:
     * Join the bids with motel names.
     */
    val enriched: DataFrame = getEnriched(sqlContext, bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = sqlContext
    .read
    .parquet(bidsPath)
    .toDF("MotelID", "BidDate", "HU", "UK", "NL", "US", "MX", "AU", "CA", "CN", "KR", "BE", "I", "JP", "IN", "HN", "GY", "DE")

  def getErroneousRecords(rawBids: DataFrame): DataFrame = rawBids.filter(rawBids("HU").startsWith("ERROR_"))
    .select("BidDate", "HU")
    .groupBy("BidDate", "HU")
    .count()

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = sqlContext.read
    .csv(exchangeRatesPath)
    .toDF("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")

  def getConvertDate = (date: String) => {
    Constants.OUTPUT_DATE_FORMAT.print(Constants.INPUT_DATE_FORMAT.parseDateTime(date))
  }

  def getCalculateEURPrice = (price: String, exchangeRates: String) => {
    var exRate: Double = exchangeRates.toDouble
    if(price.isEmpty){
      0.0
    }else {
      BigDecimal(price.toDouble * exRate).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
  }


  def getBids(sqlContext: HiveContext, rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    import sqlContext.implicits._

    val correctBidsMap = rawBids.filter(!rawBids("HU").startsWith("ERROR_"))
      .filter(!(rawBids("US").like("") && rawBids("CA").like("") && rawBids("MX").like("")))

    correctBidsMap.join(exchangeRates, correctBidsMap.col("BidDate") === exchangeRates.col("ValidFrom"))
      .select("MotelID", "BidDate", "US", "CA", "MX", "ExchangeRate")
      .flatMap(row => List(
        (row.getString(0), row.getString(1), "US", row.getString(2), row.getString(5)),
        (row.getString(0), row.getString(1), "CA", row.getString(3), row.getString(5)),
        (row.getString(0), row.getString(1), "MX", row.getString(4), row.getString(5))
      )).toDF("MotelID", "BidDate", "Losa", "price", "ExchangeRate")
      .createOrReplaceTempView("total_price")

    sqlContext.sql("SELECT MotelID, convertDate(BidDate) AS BidDate, Losa, calculateEURPrice(price, ExchangeRate) As ExchangeRate FROM total_price")
  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = sqlContext.read
    .parquet(motelsPath)
    .toDF("Motel_ID", "MotelName", "Country", "URL", "Comment")

  def getEnriched(sqlContext: HiveContext, bids: DataFrame, motels: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    import org.apache.spark.sql.expressions.Window

    val finalbids = bids.join(motels, bids.col("MotelID") === motels.col("Motel_ID")).select("MotelID", "MotelName", "BidDate", "Losa", "ExchangeRate")

    finalbids.withColumn("rowNum", row_number().over(
      Window.partitionBy("MotelID", "MotelName", "BidDate").
        orderBy($"ExchangeRate".desc)
    )).
      select("MotelID", "MotelName", "BidDate", "Losa", "ExchangeRate").
      where($"rowNum" === 1)
  }
}

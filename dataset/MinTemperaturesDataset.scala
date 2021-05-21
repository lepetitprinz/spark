import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._

object MinTemperaturesDataset {

  case class Temperature(stationID: String, data: Int, measureType: String, temperature: Float)

  def main(args: Array[String]): Unit = {

    // set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // create a SparkSession
    val spark = SparkSession
      .builder
      .appName("MinTemperatureDataset")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("data", IntegerType, nullable = true)
      .add("measureType", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    // read the file as dataset
    import spark.implicits._
    val ds = spark.read
      .schema(temperatureSchema)
      .csv("data/1800.csv")
      .as[Temperature]

    // filter out all but TMIN entries
    val minTemps = ds.filter($"measureType" === "TMIN")

    // select only (stationID, temperature)
    val stationTemps = minTemps.select("stationID", "temperature")

    // aggregate to find minimum temperature for every station
    val minTempsByStation = stationTemps.groupBy("stationID").min("temperature")

    // convert temperature to fahrenheit and sort the dataset
    val minTempsByStationF = minTempsByStation
      .withColumn("temperatureF", round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
      .select("*").sort("temperatureF")

    // Collect, format, and print the results
    val results = minTempsByStationF.collect()

    for (result <- results) {
      val station = result(0)
      val tempOrg = result(1)
      val temp = result(2).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $tempOrg / $formattedTemp")
    }
  }
}

import org.apache.spark._
import org.apache.log4j._
import scala.math.max

object MaxTemperatures{

  def parseLine(line: String): (String, String, Float) = {
    val fileds = line.split(',')
    val stationID = fileds(0)
    val entryType = fileds(2)
    val temperature = fileds(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  def main(args: Array[String]): Unit = {

    // Set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkContext
    val sc = new SparkContext("local[*]", "MaxTemperatures")

    // Read each line of data
    val lines = sc.textFile("data/1800.csv")
    val parseLines = lines.map(parseLine)

    // Filter max temperatures
    val maxTemps = parseLines.filter(x => x._2 == "TMAX")

    // Convert to (stationID, temperature)
    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))

    // Reduce by stationID retaining the minimum temperature
    val maxByStation = stationTemps.reduceByKey( (x, y) => max(x, y))

    val results = maxByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station maximum temperature: $formattedTemp")
    }
  }

}

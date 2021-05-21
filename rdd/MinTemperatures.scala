import org.apache.spark._
import org.apache.log4j._
import scala.math.min

object MinTemperatures{

	def parseLine(line: String): (String, String, Float) = {
		val fileds = line.split(",")
		val stationID = fileds(0)
		val entryType = fileds(2)
		val temperature = fileds(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
		(stationID, entryType, temperature)
	}

	def main(args: Array[String]) {

		// Set the log level to only print errors
		Logger.getLogger("org").setLevel(Level.ERROR)

		// Create a SparkContext
		val sc = new SparkContext("local[*]", "MinTemperatures")

		// Read each line of input data
		val lines = sc.textFile("data/1800.csv")
		val parseLines = lines.map(parseLine)

		// Filter out all but TMIN entries
		val minTemps = parseLines.filter(x => x._2 == "TMIN")

		// Convert to (stationID, temperature)
		val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))

		// Reduce by stationID retating the minimum temperature
		val MinTempByStation = stationTemps.reduceByKey((x, y) => min(x, y))

		// Collect, format, and print the results
		val results = MinTempByStation.collect()
		for (result <- results.sorted) {
			val station = result._1
			val temp = result._2
			val formattedTemp = f"$temp%.2f F"
			println(s"station: $station, minimum temperature: $formattedTemp")
		}
	}

}

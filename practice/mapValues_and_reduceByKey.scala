import org.apache.spark._
import org.apache.log4j._

object FriendsByAge {


	def parseLine(line: String): (Int, Int) {
		val fields = line.split(",")
		val age = fields(2).toInt
		val numFriends = fields(3).toInt

		// create a tuple
		(age, numFriends)
	}

	def main(args: Array[String]) {
		// set the log level
		Logger.getLogger("org").setLevel(ERROR)

		// create a SparkContext using every core of the local machine
		val sc = SparkContext("local[*]", "FriendsByAge")

		// Load each line of the source data into an RDD
		val lines = sc.textFile("data/fakefriends-noheader.csv")

		// Use parseLine function to convert to (age, numFriends) tuples
		val rdd = lines.map(parseLine)

		// use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
		// use reduceByKey to sum up the total numFriends and total instances for each age, by
		// adding together all the numFriends values and 1's respectively.
		val totalsByAge = rdd.mapValue(x => (x, 1)).reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2))

		// To compute the average we divide totalFriends / totalInstances for each age.	
		val averageByAge = totalsByAge.mapValue(x => x._1 / x._2 )

		// Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
		val results = averageByAge.collect()

		// Sort and print the final results.
		results.sorted.foreach(println)
	}
}
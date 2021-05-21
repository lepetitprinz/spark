import org.apache.spark._
import org.apache.log4j._

object SparkRddExample {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // create a SparkContext using every core of the local machine
    val sc = new SparkContext(master="local[*]", appName="RatingCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile(path = "data/ml-100k/u.data")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    val ratings = lines.map(x => x.split("\t")(2))

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()

    // Sort the result mpa of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each results on its own line.
    sortedResults.foreach(println)
  }
}

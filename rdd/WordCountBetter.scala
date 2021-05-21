import org.apache.spark._
import org.apache.log4j._

object WordCountBetter {

  def main(args: Array[String]): Unit = {

    // Set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")

    // Load each line of text into a RDD
    val input = sc.textFile("data/book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))

    // Convert all of words to lowercase
    val lowerCaseWords = words.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowerCaseWords.countByValue()

    // Print the results
    wordCounts.foreach(println)
  }
}

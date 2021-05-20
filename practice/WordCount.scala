import org.apache.spark._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {

  def main(args: Array[String]): Unit = {

    // Set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")

    // Read each line of my book into an RDD
    val input = sc.textFile("data/book.txt")

    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))

    // convert all of words to lower case
    val wordsLower = words.map(x => x.toLowerCase())

    // Count up the occurrences of each word
    val wordCounts = wordsLower.countByValue()

    // Filter words that appear only one
    val wordCountFiltered = wordCounts.filter(x => x._2 != 1)

    // Print the results
    wordCountFiltered.foreach(println)
  }
}

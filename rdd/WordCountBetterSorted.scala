import org.apache.spark._
import org.apache.log4j._

object WordCountBetterSorted {

  def main(args: Array[String]): Unit = {

    // Set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // create a SparkContext
    val sc = new SparkContext("local[*]", "WordCountBetterSorted")

    // Load each line of text into an RDD
    val input = sc.textFile("data/book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))

    // Convert all of words to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x, y) => x + y )

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()

    val results = wordCountsSorted.collect()

    for (result <- results) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
}

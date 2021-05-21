import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountBetterSortedDataset {

  case class Book(value: String)

  def main(args: Array[String]): Unit = {

    // set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // use SparkSession
    val spark = SparkSession
      .builder
      .appName("WordCountBetterSortedDataset")
      .master("local[*]")
      .getOrCreate()

    // read each line of text file
    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book]

    // split using a regular expression that extracts words
    val words = input
      .select(explode(split($"value", "\\W+")).alias("word"))
      .filter($"word" =!= "")

    // convert all words to lower case
    val lowercaseWords = words.select(lower($"word").alias("word"))

    // count up the occurrences of each word
    val wordCount = lowercaseWords.groupBy("word").count()

    // sort by count
    val wordCountSorted = wordCount.sort("count")

    // Show the results
    // wordCountSorted.show()
    wordCountSorted.show(wordCountSorted.count.toInt)

    // another way to do it (blending RDD;s and Datasets)
    val bookRDD = spark.sparkContext.textFile("data/book.txt")
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    val wordsDS = wordsRDD.toDS()

    val lowercaseWordsDS = wordsDS.select(lower($"value").alias("word"))
    val wordCountDS = lowercaseWordsDS.groupBy("word").count()
    val wordCountSortedDS = wordCountDS.sort("count")
    wordCountSortedDS.show(wordCountSortedDS.count.toInt)
  }
}

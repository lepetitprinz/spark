import org.apache.spark._
import org.apache.log4j._

object TotalSpentByCustomer {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val userID = fields(0).toInt
    // val itemID = fields(1).toString
    val amtOfSpent = fields(2).toFloat
    (userID, amtOfSpent)
  }

  def main(args: Array[String]) {

    // set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every local machines
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")

    val input = sc.textFile("data/customer-orders.csv")
    val parseLines = input.map(parseLine)

    val totalSpentByUser = parseLines.reduceByKey( (x,y) => (x + y) ).sortByKey()

    val results = totalSpentByUser.collect()

    for (result <- results) {
      val user = result._1
      val totalSpent = result._2
      println(s"User: $user Total: $totalSpent")
    }

  }
}

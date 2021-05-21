import org.apache.spark._
import org.apache.log4j._

object TotalSpentByCustomerSorted {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val userID = fields(0).toInt
    //val itemID = fields(1).toInt
    val amtOfSpent = fields(2).toFloat
    (userID, amtOfSpent)
  }

  def main(args: Array[String]): Unit = {

    // set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // create a SparkContext using every local machines
    val sc = new SparkContext("local[*]", "totalSpentByCustomerSorted")

    // load csv dataset
    val input = sc.textFile("data/customer-orders.csv")

    val parseLines = input.map(parseLine)

    val totalSpentByCustomer = parseLines.reduceByKey( (x,y) => (x + y))

    val totalSpentByCustomerSorted = totalSpentByCustomer.map(x => (x._2, x._1)).sortByKey()

    val results = totalSpentByCustomerSorted.collect()

    for (result <- results) {
      val totalSpent = result._1
      val user = result._2
      println(s"User: $user Total: $totalSpent")
    }
  }
}

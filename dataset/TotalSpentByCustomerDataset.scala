import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, FloatType, StructType}

object TotalSpentByCustomerDataset {

  case class Order(userID: Int, itemID: Int, spent: Float)

  def main(args: Array[String]) {

    // set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    //
    val spark = SparkSession
      .builder
      .appName("TotalSpentByCustomerDataset")
      .master("local[*]")
      .getOrCreate()

    val orderSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("itemID", IntegerType, nullable = true)
      .add("spent", FloatType, nullable = true)

    import spark.implicits._
    val ds = spark.read
      .schema(orderSchema)
      .csv("data/customer-orders.csv")
      .as[Order]

    val userSpent = ds.select("userID", "spent")
    val totalSpentByUser = userSpent.groupBy("userID").sum("spent")
    val totalSpentByUserSorted = totalSpentByUser.select("*").sort("spent")

    val results = totalSpentByUserSorted.collect()

    for (result <- results) {
      val user = result(0)
      val totSpent = result(1)
      println(s"user: $user Total Spend: $totSpent")
    }
  }
}

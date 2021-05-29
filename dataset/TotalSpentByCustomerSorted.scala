import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.functions._
import org.apache.log4j._

object TotalSpentByCustomerSorted{

  case class Customer(cust_id: Int, item_id: Int, amount_spent: Double)

  def main(args: Array[String]): Unit = {

    // Set log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("TotalSpentByCustomer")
      .master("local[*]")
      .getOrCreate()

    val customerSchema = new StructType()
      .add("cust_id", IntegerType, nullable = true)
      .add("item_id", IntegerType, nullable = true)
      .add("amount_spent", DoubleType, nullable = true)

    // class to infer the schema
    import spark.implicits._
    val ds = spark.read
      .schema(customerSchema)
      .csv("/Users/yjkim/data/spark/customer-orders.csv")
      .as[Customer]

    val totalByCustomer = ds
      .groupBy("cust_id")
      .agg(round(sum("amount_spent"), 2)
        .alias("total_spent"))

    val totalByCustomerSorted = totalByCustomer.sort("total_spent")

    totalByCustomerSorted.show(totalByCustomer.count.toInt)
  }
}

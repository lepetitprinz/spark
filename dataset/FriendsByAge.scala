import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FriendsByAge {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {

    // set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // use a new SparkSession
    val spark = SparkSession
      .builder
      .appName("FriendsByAge")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    // select only age, numFriends columns
    val friendsByAge = ds.select("age", "friends")

    // group by "age" and compute average
    friendsByAge.groupBy("age").avg("friends").show()

   friendsByAge.groupBy("age").agg(round(avg("friends"), 2)
      .alias("friends_avg")).sort("age").show()
  }
}

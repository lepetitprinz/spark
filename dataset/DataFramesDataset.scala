import org.apache.spark.sql._
import org.apache.log4j._

object DataFramesDataset {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  /** main function */
  def main(args: Array[String]): Unit = {

    // set log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // use new SparkSession interface
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // convert csv file to a DataSet, using Person case
    // class to infer the schema
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    // There are lots of other ways to make a DataFrame
    // for example, spark.read.json("json file path") or sqlContext.table("hive table name")

    println("Here is our inferred schema")
    people.printSchema()

    println("Let's select the name column:")
    people.select("name").show()

    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()

    println("Group by age:")
    people.groupBy("age").count().show()

    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()

    spark.stop()
  }
}

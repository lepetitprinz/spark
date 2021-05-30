import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object PopularMoviesDataset {

  final case class Movie(movieId: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._

    // Load movie data as dataset
    val moviesDs = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("/Users/yjkim/data/spark/ml-100k")
      .as[Movie]

    // popularity
    val topMovieIDs = moviesDs.groupBy("movieID").count().orderBy(desc("count"))

    // Grab the top 10
    topMovieIDs.show(10)

    // stop the session
    spark.stop()
  }

}

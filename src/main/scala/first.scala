import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

/**
  * Created by Gabor on 02/04/2017.
  */
object first {
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("process-openaddr")
    .getOrCreate()


  def main(args: Array[String]): Unit = {

    val data = spark.read.option("header", "true").csv("C:\\Users\\Gabor\\IdeaProjects\\resources\\openaddr-collected-us_northeast/summary/us/**/*.csv").withColumn("filename", input_file_name)
    data.createOrReplaceTempView("data")
    val stats = spark.sql("SELECT filename, COUNT(1) FROM data GROUP BY filename")
    stats.show(100, truncate = false)
  }
}

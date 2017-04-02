import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Gabor on 02/04/2017.
  */
object first {
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("process-openaddr")
    .getOrCreate()

  private val basePath = "C:/Users/Gabor/IdeaProjects/resources/"

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\Users\\Gabor\\Hadoop\\hadoop-2.7.1\\")

    val customSchema = StructType(Array(
      StructField("count", IntegerType, nullable = true),
      StructField("lon", DoubleType, nullable = true),
      StructField("lat", DoubleType, nullable = true),
      StructField("area", StringType, nullable = true)))

    val data = spark.read.option("header", "true").schema(customSchema).csv(basePath + "openaddr-collected-us_northeast/summary/us/**/*.csv").withColumn("filename", input_file_name)
    //data.createOrReplaceTempView("data")
    //val stats = spark.sql("SELECT filename, COUNT(1) FROM data GROUP BY filename")

    data.show(100, truncate = false)
    data.printSchema
    data.write.mode(SaveMode.Overwrite).parquet(basePath + "summary.parquet")
  }
}

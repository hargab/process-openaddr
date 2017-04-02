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

    loadAndSaveFull()
  }

  def loadAndSaveSummary(): Unit = {
    val customSchema = StructType(Array(
      StructField("count", IntegerType, nullable = true),
      StructField("lon", DoubleType, nullable = true),
      StructField("lat", DoubleType, nullable = true),
      StructField("area", StringType, nullable = true)))

    val data = spark.read.option("header", "true").schema(customSchema).csv(basePath + "openaddr-collected-us_northeast/summary/us/**/*.csv").withColumn("filename", input_file_name)
    data.show(100, truncate = false)
    data.printSchema
    data.write.mode(SaveMode.Overwrite).parquet(basePath + "summary.parquet")
  }

  def loadAndSaveFull(): Unit = {
    val customSchema = StructType(Array(
      StructField("lon", new DecimalType(10, 7), nullable = true),
      StructField("lat", new DecimalType(10, 7), nullable = true),
      StructField("number", StringType, nullable = true),
      StructField("street", StringType, nullable = true),
      StructField("unit", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("district", StringType, nullable = true),
      StructField("region", StringType, nullable = true),
      StructField("postcode", IntegerType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("hash", StringType, nullable = true)
    ))

    val path = "openaddr-collected-us_northeast/us/**/*.csv"
    val data = spark.read.option("header", "true").option("treatEmptyValuesAsNulls", "true").option("mode", "DropMalformed").schema(customSchema).csv(basePath + path).withColumn("filename", input_file_name)
    data.write.mode(SaveMode.Overwrite).parquet(basePath + "us_northeast.parquet")
  }

  def isNumber(s: String): Boolean = {
    try {
      s.toDouble
      return true
    } catch {
      case _ => return false
    }
  }
}

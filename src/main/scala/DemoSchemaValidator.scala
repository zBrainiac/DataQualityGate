import com.amazon.deequ.schema.{RowLevelSchema, RowLevelSchemaValidator}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DemoSchemaValidator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DemoSchemaValidator")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val input_filename: String = "amazon_reviews_us_Jewelry_v1_00.tsv"
    val input_path: String = "./data/input/"
    val output_path: String = "./data/output/"

    val dataset = spark.read
      .option("header", value = true)
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .csv(input_path + input_filename)

    dataset.printSchema()

    println("RAW dataset:")
    dataset.show(10)

    val schema = RowLevelSchema()
      .withIntColumn("customer_id", isNullable = false)
      .withStringColumn(name = "product_id", isNullable = false, matches = Some("^[A-Z0-9]+$"))
      .withIntColumn("product_parent", isNullable = false)
      .withStringColumn("vine", isNullable = false, matches = Option("N|Y"))
      .withTimestampColumn("review_date", mask = "yyyy-MM-dd", isNullable = false)

    val result = RowLevelSchemaValidator.validate(dataset, schema)

    println("numValidRows: " + result.numValidRows)
    println("numInvalidRows: " + result.numInvalidRows)

    println("Invalid Rows:")

    result.invalidRows.show(truncate = true)

    result.validRows.write.mode(SaveMode.Overwrite)
      .orc(output_path + "test_DemoSchemaValidator_valid_orc")

    result.validRows.write.mode(SaveMode.Overwrite)
      .csv(output_path + "test_DemoSchemaValidator_valid_csv")

    result.invalidRows.write.mode(SaveMode.Overwrite)
      .csv(output_path + "test_DemoSchemaValidator_invalid")

  }
}

import com.amazon.deequ.schema.{RowLevelSchema, RowLevelSchemaValidator}
import io.prometheus.client.exporter.PushGateway
import io.prometheus.client.{CollectorRegistry, Gauge}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.rogach.scallop.{LazyMap, ScallopConf}

object abc_SchemaValidator_4711_Monitoring {

  val job_start_time: Long = System.currentTimeMillis / 1000
  println("job_start_time: "+ job_start_time)


  class Conf(args: Seq[String])
    extends ScallopConf(args) {
    val propsMap: LazyMap[String, String] = props[String]('P')
    verify()
  }

  val registry = new CollectorRegistry



  def main(args: Array[String]): Unit = {

    val object_name = getClass.getName
    val name = object_name.substring(0, object_name.length -1)
    println("name: " + name)

    val conf = new Conf(args)
    val input_filename: String = conf.propsMap("input_filename")
    val input_path: String = conf.propsMap("input_path")
    val output_path: String = conf.propsMap("output_path")

    println("input_filename is: " + input_filename)
    println("input_path is: " + input_path)
    println("output_path is: " + output_path)

    val spark = SparkSession
      .builder()
      .appName(name)
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

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

    // ValidRows
    println(s"\nnumValidRows: " + result.numValidRows)

    val numValidRowsCounter: Gauge = Gauge
      .build()
      .name("num_Valid_Rows_count")
      .help("Num of valid rows in dataset")
      .labelNames("dataset")
      .register(registry)

  //  numValidRowsCounter.labels(input_filename).inc(result.numValidRows)
    numValidRowsCounter.labels(input_filename).set(result.numValidRows)

    // InvalidRows
    println(s"\nnumInvalidRows: " + result.numInvalidRows)
    val numInValidRowsCounter: Gauge = Gauge
      .build()
      .name("num_Invalid_Rows_count")
      .help("Num of invalid rows in dataset")
      .labelNames("dataset")
      .register(registry)

    numInValidRowsCounter.labels(input_filename).set(result.numInvalidRows)


    println(s"\nInvalid Rows:")
    result.invalidRows.show(truncate = true)

    result.validRows.write.mode(SaveMode.Overwrite)
      .orc(output_path + name + "_valid_orc")

    result.validRows.write.mode(SaveMode.Overwrite)
      .csv(output_path + name +  "_valid_csv")

    result.invalidRows.write.mode(SaveMode.Overwrite)
      .csv(output_path + name + "_invalid")

    val pg = new PushGateway("localhost:9091")

    val job_end_time: Long = System.currentTimeMillis / 1000
    println("job_end_time: "+ job_end_time)
    val job_duration: Long = job_end_time - job_start_time
    println("job_duration: "+ job_duration)


    val job_total_duration_sec: Gauge = Gauge
      .build()
      .name("job_total_duration_sec")
      .help("job duration")
      .labelNames("dataset")
      .register(registry)

    job_total_duration_sec.labels(input_filename).set(job_duration)

    pg.pushAdd(registry, name)
  }

}

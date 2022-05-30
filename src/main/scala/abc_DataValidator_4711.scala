import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{LazyMap, ScallopConf}

object abc_DataValidator_4711 {

  class Conf(args: Seq[String])
    extends ScallopConf(args) {
    val propsMap: LazyMap[String, String] = props[String]('P')
    verify()
  }

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    val input_filename: String = conf.propsMap("input_filename")
    val input_path: String = conf.propsMap("input_path")
    val output_path: String = conf.propsMap("output_path")

    println("input_filename is: " + input_filename)
    println("input_path is: " + input_path)
    println("output_path is: " + output_path)

    val spark = SparkSession
      .builder()
      .appName("abc_DataValidator_4711")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val dataset = spark.read
      .option("header", value = true)
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .csv(input_path + input_filename)

    val metricsFile = output_path + "metrics/metrics.json"
    val repository = FileSystemMetricsRepository(spark, metricsFile)

    val resultKey = ResultKey(System.currentTimeMillis(), Map("tag" -> "repositoryExample"))

    dataset.printSchema()
    println("RAW dataset:")
    dataset.show(10)

    val verificationResult = VerificationSuite()
      .onData(dataset)
      .addCheck(
        Check(CheckLevel.Error, "integrity checks")
          .hasSize(_ == 5)
          .isComplete("review_id")
          .isUnique("review_id")
          .isComplete("customer_id")
          .isContainedIn("vine", Array("Y", "N"))
          .isNonNegative("total_votes"))
      .addCheck(
        Check(CheckLevel.Warning, "distribution checks")
          .hasApproxQuantile("total_votes", 0.5, _ <= 10))
      .useRepository(repository)
      .saveOrAppendResult(resultKey)
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data, the following constraints were not satisfied:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter {
          _.status != ConstraintStatus.Success
        }
        .foreach { result =>
          println(s"${result.constraint} failed: ${result.message.get}")
        }
    }


    println(s"\nMetrics of the 10 : ")

    repository.load()
      .withTagValues(Map("tag" -> "repositoryExample"))
      .getSuccessMetricsAsDataFrame(spark)
      .show(numRows = 70, truncate = false)
  }
}

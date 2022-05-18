import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.google.common.io.Files
import org.apache.spark.sql.SparkSession

import java.io.File

object DemoDataValidator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DemoDataValidator")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val input_filename: String = "amazon_reviews_us_Jewelry_v1_00.tsv"
    val input_path: String = "../DataQuality/data/input/"
    val output_path: String = "../DataQuality/data/output/"


    val dataset = spark.read
      .option("header", value = true)
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .csv(input_path + input_filename)

    val metricsFile = new File(Files.createTempDir(), "metrics.json")
    val repository = FileSystemMetricsRepository(spark, metricsFile.getAbsolutePath)
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

  }
}

import com.amazon.deequ.profiles.{ColumnProfilerRunner, NumericColumnProfile}
import org.apache.spark.sql.SparkSession

import java.time.Instant

object DemoDataProfiler {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DemoDataProfiler")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val input_filename: String = "amazon_reviews_us_Jewelry_v1_00.tsv"
    val input_path: String = "./data/input/"
    val output_path: String = "./data/output/"

    val current_tsd: String = Instant.now().toString
      .replace(":", "")

    val dataset = spark.read
      .option("header", value = true)
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .csv(input_path + input_filename)

    val metric_file = "data/output/profile/" + current_tsd + "_" + input_filename + ".json"

    dataset.printSchema()
    println("RAW dataset:")
    dataset.show(10)

    val result = ColumnProfilerRunner()
      .onData(dataset)
      .useSparkSession(spark)
      .saveColumnProfilesJsonToPath(metric_file)
      .run()

    result.profiles.foreach { case (productName, profile) =>
      println(s"Column '$productName':\n " +
        s"\tcompleteness: ${profile.completeness}\n" +
        s"\tapproximate number of distinct values: ${profile.approximateNumDistinctValues}\n" +
        s"\tdatatype: ${profile.dataType}\n")
    }

    /* For numeric columns, we get descriptive statistics */
    val total_votes_Profile = result.profiles("total_votes").asInstanceOf[NumericColumnProfile]
    println(s"Statistics of 'total_votes':\n" +
      s"\tminimum: ${total_votes_Profile.minimum.get}\n" +
      s"\tmaximum: ${total_votes_Profile.maximum.get}\n" +
      s"\tmean: ${total_votes_Profile.mean.get}\n" +
      s"\tstandard deviation: ${total_votes_Profile.stdDev.get}\n")

    /* For string columns with a low number of distinct values, we get the full value distribution. */
    val star_rating_Profile = result.profiles("star_rating")
    println("Value distribution in 'total_votes':")
    star_rating_Profile.histogram.foreach {
      _.values.foreach { case (key, entry) =>
        println(s"\t$key occurred ${entry.absolute} times (ratio is ${entry.ratio})")
      }
    }

    val vine_Profile = result.profiles("vine")
    println("Value distribution in 'vine':")
    vine_Profile.histogram.foreach {
      _.values.foreach { case (key, entry) =>
        println(s"\t$key occurred ${entry.absolute} times (ratio is ${entry.ratio})")
      }
    }

  }
}

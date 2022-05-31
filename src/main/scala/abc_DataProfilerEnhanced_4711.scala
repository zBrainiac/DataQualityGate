import com.amazon.deequ.profiles.{ColumnProfilerRunner, NumericColumnProfile}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.rogach.scallop.{LazyMap, ScallopConf}

import java.time.Instant

object abc_DataProfilerEnhanced_4711 {

  class Conf(args: Seq[String])
    extends ScallopConf(args) {
    val propsMap: LazyMap[String, String] = props[String]('P')
    verify()
  }

  def main(args: Array[String]): Unit = {

    val object_name = getClass.getName
    val name = object_name.substring(0, object_name.length -1)
    println("object_name: " + name)
    
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

    val current_tsd: String = Instant.now().toString
      .replace(":", "")

    val epoch_tsd: Long = System.currentTimeMillis / 1000

    val dataset = spark.read
      .option("header", value = true)
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .csv(input_path + input_filename)

    val metric_file = output_path + "profile/" + current_tsd + "_" + input_filename + ".json"

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


    println("Dataframe with all profile information:")
    import spark.implicits._

    val profileResultDataset = result.profiles.map {
      case (productName, profile) => (
        productName,
        profile.completeness,
        profile.dataType.toString,
        profile.approximateNumDistinctValues)
    }.toSeq.toDS

    val finalDataset = profileResultDataset
      .withColumnRenamed("_1", "column")
      .withColumnRenamed("_2", "completeness")
      .withColumnRenamed("_3", "inferred_datatype")
      .withColumnRenamed("_4", "approx_distinct_values")
      .withColumn("timestamp", lit(epoch_tsd))

    finalDataset.show(false)

    println("approx_distinct_values with low value distribution:")
    val low_val_dist = finalDataset.filter(finalDataset("approx_distinct_values") < 50)
      .select("column")

    low_val_dist.show(false)
  }
}

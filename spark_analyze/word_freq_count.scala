import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.util.Calendar

object ReviewWordFrequency {
  def main(args: Array[String]): Unit = {
    // Record start time
    val startTime = Calendar.getInstance().getTimeInMillis

    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("ReviewWordFrequencyWithMLlib")
      .master("local[*]")  // Remove in cluster environment
      .getOrCreate()
    import spark.implicits._

    // ======================
    // 1. Read data
    // ======================
    // 1.1 Read clustering result Parquet (contains imdb_id and category)
    val parquetPath = "/data/movie_cluster_with_category.parquet"  // Replace with your Parquet path
    val clusterDF = spark.read.parquet(parquetPath)
      .select("imdb_id", "category")
      .filter(col("imdb_id").isNotNull && col("category").isNotNull)  // Filter invalid data

    // 1.2 Read review CSV files (multiple CSVs, join by imdb_id)
    val csvPath = "/data/reviews_dir/"  // Replace with your CSV folder path
    val reviewDF = spark.read
      .option("header", "true")       // CSV contains header
      .option("quote", "\"")          // Handle quoted fields
      .option("escape", "\"")         // Escape quotes within fields
      .csv(csvPath)                   // Read all CSVs in the folder
      .select(
        col("imdb_id"),
        col("title"),
        // 关键修改1：先移除数组符号，再清洗所有标点（保留字母、数字和空格）
        regexp_replace(
          regexp_replace(col("reviews_c"), "[\\[\\]']", ""),  // 先去掉[]'
          "[^a-zA-Z0-9\\s]", ""  // 再移除所有非字母、非数字、非空格的字符（含,.?等）
        ).alias("reviews_c_str"),
        regexp_replace(
          regexp_replace(col("reviews_u"), "[\\[\\]']", ""),
          "[^a-zA-Z0-9\\s]", ""
        ).alias("reviews_u_str")
      )
      .filter(col("imdb_id").isNotNull)  // Filter records without imdb_id

    // ======================
    // 2. Join data (by imdb_id)
    // ======================
    val joinedDF = reviewDF
      .join(clusterDF, Seq("imdb_id"), "inner")  // Inner join, keep only data existing in both sides
      .select(
        "imdb_id", "title", "category",
        "reviews_c_str", "reviews_u_str"
      )

    // ======================
    // 3. Word segmentation (using Spark MLlib)
    // ======================
    // 3.1 Professional reviews (reviews_c) segmentation
    val tokenizerC = new Tokenizer()
      .setInputCol("reviews_c_str")  // Input: cleaned review string
      .setOutputCol("c_tokens")      // Output: original segmentation results (including stop words)

    val stopRemoverC = new StopWordsRemover()
      .setInputCol("c_tokens")       // Input: original tokens
      .setOutputCol("c_words")       // Output: tokens after stop word removal
      .setCaseSensitive(false)       // Case insensitive

    // 3.2 User reviews (reviews_u) segmentation
    val tokenizerU = new Tokenizer()
      .setInputCol("reviews_u_str")
      .setOutputCol("u_tokens")

    val stopRemoverU = new StopWordsRemover()
      .setInputCol("u_tokens")
      .setOutputCol("u_words")
      .setCaseSensitive(false)

    // Execute segmentation and stop word filtering
    val processedDF = stopRemoverC.transform(  // Filter stop words for professional reviews
      stopRemoverU.transform(                  // Filter stop words for user reviews
        tokenizerC.transform(                  // Segment professional reviews
          tokenizerU.transform(joinedDF)       // Segment user reviews
        )
      )
    )
    // Expand segmentation results (one word per row)
    .withColumn("c_word", explode(col("c_words")))  // Professional review words
    .withColumn("u_word", explode(col("u_words")))  // User review words
    .filter("c_word != '' and u_word != ''")  // Filter empty strings

    // ======================
    // 4. Define target categories
    // ======================
    val categories = Array(
      "high_tomatometer_high_audience",
      "high_tomatometer_low_audience",
      "low_tomatometer_high_audience",
      "low_tomatometer_low_audience"
    )

    // ======================
    // 5. Word frequency statistics and output (professional reviews)
    // ======================
    categories.foreach { category =>
      // Calculate word frequency for current category
      val wordFreq = processedDF
        .filter(col("category") === category)
        .groupBy("c_word")
        .count()
        .withColumnRenamed("c_word", "word")
        .withColumnRenamed("count", "word_occurrences")
        .orderBy(col("word_occurrences").desc)  // Sort by frequency descending

      // Calculate total words (sum of all word occurrences in current category)
      val totalWords = if (wordFreq.isEmpty) 0L else wordFreq.agg(sum("word_occurrences")).head().getLong(0)

      // Add total words column and output
      val result = wordFreq.withColumn("total_word_occurrences", lit(totalWords))

      // Output path
      val outputPath = s"${category}_review_c.csv"
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      if (fs.exists(new Path(outputPath))) fs.delete(new Path(outputPath), true)  // Delete old files

      // Write to CSV
      result.write
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(outputPath)

      println(s"Generated professional review word frequency file: $outputPath (Total words: $totalWords)")
    }

    // ======================
    // 6. Word frequency statistics and output (user reviews)
    // ======================
    categories.foreach { category =>
      val wordFreq = processedDF
        .filter(col("category") === category)
        .groupBy("u_word")
        .count()
        .withColumnRenamed("u_word", "word")
        .withColumnRenamed("count", "word_occurrences")
        .orderBy(col("word_occurrences").desc)

      val totalWords = if (wordFreq.isEmpty) 0L else wordFreq.agg(sum("word_occurrences")).head().getLong(0)
      val result = wordFreq.withColumn("total_word_occurrences", lit(totalWords))

      val outputPath = s"${category}_review_u.csv"
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      if (fs.exists(new Path(outputPath))) fs.delete(new Path(outputPath), true)

      result.write
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(outputPath)

      println(s"Generated user review word frequency file: $outputPath (Total words: $totalWords)")
    }

    // Calculate and print total execution time
    val endTime = Calendar.getInstance().getTimeInMillis
    val executionTimeSeconds = (endTime - startTime) / 1000.0
    println(s"Total execution time: $executionTimeSeconds seconds")

    spark.stop()
  }
}

ReviewWordFrequency.main(Array())
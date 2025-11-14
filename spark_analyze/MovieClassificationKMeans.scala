import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.linalg.DenseVector

object MovieClassificationKMeans {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MovieFourClassificationKMeans")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 1. Read multiple Parquet files
    val parquetDir = "/data/movie_parquets/"
    val rawDF = spark.read.parquet(parquetDir)
    println(s"Successfully read Parquet files, total records: ${rawDF.count()}")

    // 2. Data preprocessing: Filter valid data + Convert all array fields to strings (core fix)
    val processedDF = rawDF
      // Filter records with complete ratings
      .filter(col("rt_tomatometer_rating").isNotNull && col("rt_audience_rating").isNotNull)
      // Calculate rating discrepancy and average rating
      .withColumn("rating_discrepancy", abs(col("rt_tomatometer_rating") - col("rt_audience_rating")))
      .withColumn("avg_rating", (col("rt_tomatometer_rating") + col("rt_audience_rating")) / 2)
      // Convert array to string: genres_tmdb (movie genres)
      .withColumn("genres_tmdb_str", concat_ws(",", col("genres_tmdb")))
      .drop("genres_tmdb")
      .withColumnRenamed("genres_tmdb_str", "genres_tmdb")
      // Convert array to string: main_cast_names (main cast names)
      .withColumn("main_cast_names_str", concat_ws(",", col("main_cast_names")))
      .drop("main_cast_names")
      .withColumnRenamed("main_cast_names_str", "main_cast_names")
      // Convert array to string: main_cast_characters (main cast characters)
      .withColumn("main_cast_characters_str", concat_ws(",", col("main_cast_characters")))
      .drop("main_cast_characters")
      .withColumnRenamed("main_cast_characters_str", "main_cast_characters")
      // Convert array to string: director_names (director names)
      .withColumn("director_names_str", concat_ws(",", col("director_names")))
      .drop("director_names")
      .withColumnRenamed("director_names_str", "director_names")
      // Convert array to string: writer_names (writer names)
      .withColumn("writer_names_str", concat_ws(",", col("writer_names")))
      .drop("writer_names")
      .withColumnRenamed("writer_names_str", "writer_names")
      // Convert array to string: producer_names (producer names)
      .withColumn("producer_names_str", concat_ws(",", col("producer_names")))
      .drop("producer_names")
      .withColumnRenamed("producer_names_str", "producer_names")

    println(s"Valid records after filtering: ${processedDF.count()} (require both critic and audience ratings)")

    // 3. Feature engineering
    val assembler = new VectorAssembler()
      .setInputCols(Array("rt_tomatometer_rating", "rt_audience_rating"))
      .setOutputCol("features")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithMean(true)
      .setWithStd(true)

    // 4. KMeans clustering
    val kmeans = new KMeans()
      .setK(4)
      .setSeed(42)
      .setMaxIter(100)
      .setFeaturesCol("scaledFeatures")
      .setPredictionCol("cluster_id")

    val pipeline = new Pipeline().setStages(Array(assembler, scaler, kmeans))
    val model = pipeline.fit(processedDF)
    val predictionDF = model.transform(processedDF)

    // 5. Print cluster centers
    val clusterCenters = model.stages(2).asInstanceOf[KMeansModel].clusterCenters
    println("\nCluster centers (standardized):")
    clusterCenters.foreach { center =>
      println(center.asInstanceOf[DenseVector].values.mkString(","))
    }

    // 6. Classification logic
    val classifyMovie = udf((avgRating: Double, discrepancy: Double) => {
      val highScoreThreshold = 70.0
      val highDiscrepancyThreshold = 15.0
      (avgRating >= highScoreThreshold, discrepancy >= highDiscrepancyThreshold) match {
        case (true, true) => "High Score & High Discrepancy"
        case (true, false) => "High Score & Low Discrepancy"
        case (false, true) => "Low Score & High Discrepancy"
        case (false, false) => "Low Score & Low Discrepancy"
      }
    })

    // 7. Build result DataFrame (fix: remove duplicate rating_discrepancy)
    val resultDF = predictionDF
      .withColumn("movie_category", classifyMovie(col("avg_rating"), col("rating_discrepancy")))
      .select(
        (processedDF.columns.map(col) ++ Seq(
          col("avg_rating").alias("average_rating"),  // Only keep non-duplicate fields
          col("cluster_id").alias("cluster_id"),
          col("movie_category").alias("movie_category")
        )): _*
      )

    // 8. Export CSV (all fields are basic types, export normally)
    val outputPath = "/data/movie_classification_result.csv"
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(outputPath))) fs.delete(new Path(outputPath), true)

    resultDF.write
      .mode("overwrite")
      .option("header", "true")
      .option("encoding", "utf-8")
      .csv(outputPath)

    // 9. Print classification statistics
    println("\nMovie Category Statistics:")
    resultDF.groupBy("movie_category")
      .agg(
        count("*").alias("count"),
        round(count("*") / resultDF.count() * 100, 2).alias("percentage(%)")
      )
      .show(false)

    // 10. Prompt to merge CSV files
    println(s"\nResults exported to directory: $outputPath")
    println("Command to merge into a single CSV file:")
    println(s"cat $outputPath/*.csv > /data/movie_classification_full.csv")

    spark.stop()
  }
}

// Auto-execute
MovieClassificationKMeans.main(Array())
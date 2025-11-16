import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler, StandardScalerModel}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.linalg.DenseVector

object ClusterToRatingMapping {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ClusterToRatingMapping")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 1. Read data and preprocess
    val parquetDir = "/data/movie_parquets/"
    val rawDF = spark.read.parquet(parquetDir)
    val processedDF = rawDF
      .filter(col("rt_tomatometer_rating").isNotNull && col("rt_audience_rating").isNotNull)

    // 2. Feature engineering (assembling + standardization)
    val assembler = new VectorAssembler()
      .setInputCols(Array("rt_tomatometer_rating", "rt_audience_rating"))
      .setOutputCol("features")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithMean(true)
      .setWithStd(true)

    // 3. Train KMeans model
    val kmeans = new KMeans()
      .setK(4)
      .setSeed(42)
      .setFeaturesCol("scaledFeatures")
      .setPredictionCol("cluster_id")

    val pipeline = new Pipeline().setStages(Array(assembler, scaler, kmeans))
    val model = pipeline.fit(processedDF)
    val predictionDF = model.transform(processedDF)

    // 4. Extract mean and standard deviation from the standardization model (for restoring original ratings)
    val scalerModel = model.stages(1).asInstanceOf[StandardScalerModel]
    val mean = scalerModel.mean  // Mean of original features [tomatometer rating mean, audience rating mean]
    val std = scalerModel.std    // Standard deviation of original features [tomatometer rating std, audience rating std]

    // 5. Extract cluster centers (after standardization) and convert to original ratings
    val kmeansModel = model.stages(2).asInstanceOf[KMeansModel]
    val clusterCenters = kmeansModel.clusterCenters  // Standardized cluster centers (4 clusters)

    // Convert each cluster center to original rating range
    val clusterMapping = clusterCenters.zipWithIndex.map { case (center, clusterId) =>
      val tomometerRaw = center(0) * std(0) + mean(0)  // Restore tomatometer rating
      val audienceRaw = center(1) * std(1) + mean(1)    // Restore audience rating
      (clusterId, tomometerRaw, audienceRaw)
    }.toSeq.toDF("cluster_id", "center_tomometer", "center_audience")

    // 6. Define threshold for high/low ratings (e.g., 70 points, adjust based on actual data distribution)
    val highThreshold = 60.0

    // 7. Label each cluster center with category (both high/critic high audience low/etc.)
    val clusterToCategory = clusterMapping
      .withColumn("category", 
        when(col("center_tomometer") >= highThreshold && col("center_audience") >= highThreshold, 
          "high_tomatometer_high_audience")
        .when(col("center_tomometer") >= highThreshold && col("center_audience") < highThreshold, 
          "high_tomatometer_low_audience")
        .when(col("center_tomometer") < highThreshold && col("center_audience") >= highThreshold, 
          "low_tomatometer_high_audience")
        .otherwise("low_tomatometer_low_audience")
      )

    // 8. Print mapping between cluster IDs and rating categories (key result)
    println("=== Mapping between Cluster ID and Rating Category ===")
    clusterToCategory.show(false)

    // 9. Associate mapping with original data to get each movie's category
    val finalDF = predictionDF
      .join(clusterToCategory.select("cluster_id", "category"), "cluster_id")
      .drop("features", "scaledFeatures", "cluster_id")
    finalDF.show(5)
    
    // 10. Output results (Parquet format)
    val outputPath = "/data/movie_cluster_with_category.parquet"
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(outputPath))) fs.delete(new Path(outputPath), true)

    finalDF.write.mode("overwrite").parquet(outputPath)
    println(s"\nResults saved to: $outputPath")

    // 11. Print number of movies in each category (verify results)
    println("\n=== Number of Movies in Each Category ===")
    finalDF.groupBy("category").count().show(false)

    spark.stop()
  }
}

ClusterToRatingMapping.main(Array())
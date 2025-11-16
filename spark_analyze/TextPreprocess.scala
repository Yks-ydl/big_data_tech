import org.apache.spark.sql.{DataFrame}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

// 1. 配置文件路径（**必须修改为你的实际路径**）
val allCsvPath = "all.csv"  // 例："C:/data/all.csv" 或 "/home/user/all.csv"
val reviewsDir = "reviews_dir/"  // 所有评论CSV的存放目录
val outputPath = "output_merged_single.csv"  // 最终输出的单个CSV文件路径

// 2. 读取主文件 all.csv（只保留必要字段，优化性能）
val allDF = spark.read
  .option("header", "true")
  .option("encoding", "UTF-8")
  .option("quote", "\"")
  .option("escape", "\"")
  .csv(allCsvPath)
  .select("imdb_id", "movie_category")
  .repartition(8)  // 预分区，加速后续并行关联

// 3. 批量并行读取所有评论CSV，增加引号处理配置
val reviewsDF = spark.read
  .option("header", "true")
  .option("encoding", "UTF-8")
  .option("quote", "\"")       // 设置引号字符
  .option("escape", "\"")      // 设置转义字符为引号
  .option("multiLine", "true") // 支持多行记录
  .csv(s"$reviewsDir/*.csv")  // 通配符匹配目录下所有同结构CSV

// 4. 检查必要字段（避免结构错误）
val requiredCols = Seq("title")
val missingCols = requiredCols.filterNot(reviewsDF.columns.contains)
if (missingCols.nonEmpty) {
  throw new IllegalArgumentException(s"评论文件缺少必填字段：${missingCols.mkString(", ")}")
}

// // 5. 清理评论内容中的引号和特殊字符，避免CSV格式问题
// val cleanReviewsDF = reviewsDF
//   .withColumn("reviews_c", regexp_replace(col("reviews_c"), "\"", "\\\\\""))  // 转义双引号
//   .withColumn("reviews_u", regexp_replace(col("reviews_u"), "\"", "\\\\\""))
//   .withColumn("title", regexp_replace(col("title"), "\"", "\\\\\""))

// 6. 配置并行运算参数（按CPU核心数调整，8核→16）
spark.conf.set("spark.sql.shuffle.partitions", 16)
spark.conf.set("spark.default.parallelism", 16)

// 7. 并行关联 movie_category 列
val mergedDF = cleanReviewsDF
  .join(allDF, Seq("imdb_id"), "left_outer")  // 左连接保留所有评论行

// 8. 清理已存在的输出文件（避免冲突）
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
if (fs.exists(new Path(outputPath))) {
  fs.delete(new Path(outputPath), true)
}

// 9. 并行处理后合并为单个CSV（关键：coalesce(1) 合并分区）
mergedDF
  .coalesce(1)  // 将所有并行分区合并为1个，输出单个文件
  .write
  .option("header", "true")
  .option("encoding", "UTF-8")
  .option("quote", "\"")       // 输出时使用引号
  .option("escape", "\"")      // 输出时使用引号作为转义符
  .option("quoteAll", "true")  // 所有字段都用引号包裹
  .mode("overwrite")
  .csv(outputPath + "_temp")  // 先写入临时目录

// 10. 重命名临时文件为目标文件名
val tempDir = new Path(outputPath + "_temp")
val partFiles = fs.listStatus(tempDir)
  .filter(_.getPath.getName.startsWith("part-00000"))  // 找到合并后的分片文件
  .map(_.getPath)

if (partFiles.nonEmpty) {
  val sourcePath = partFiles(0)
  val targetPath = new Path(outputPath)
  fs.rename(sourcePath, targetPath)  // 重命名为最终的单个CSV文件
  fs.delete(tempDir, true)  // 删除临时目录
}

// 11. 打印处理结果统计
println(s"=== 处理完成！ ===")
println(s"处理的评论文件总数：${fs.listStatus(new Path(reviewsDir)).count(_.getPath.getName.endsWith(".csv"))}")
println(s"总记录数：${reviewsDF.count()}")
println(s"成功匹配 movie_category 的记录数：${mergedDF.filter($"movie_category".isNotNull).count()}")
println(s"未匹配的记录数：${mergedDF.filter($"movie_category".isNull).count()}")
println(s"最终单个CSV文件路径：$outputPath")

// 自动退出 spark-shell
sys.exit(0)
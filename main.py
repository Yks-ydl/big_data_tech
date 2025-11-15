#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# build_abt.py
#
# 用法（本地或集群均可）：
#   spark-submit --driver-memory 6g --executor-memory 6g build_abt.py
#
# 说明：
# - ABT 主键为 links.csv 标准化后的 imdb_id（形如 tt0114709）
# - 支柱3 & 支柱4 采用基于标题 token 的 Jaccard 相似度 + 年份接近度的模糊匹配
# - 文本评论（IMDB 50K Reviews）若缺少 imdb_id 字段，将产生空聚合（列为 NULL）
# - 结果输出：./abt_movies.parquet

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
import re
import json
hdfs_base_path = "hdfs://master:9000"
data_paths = {
    'movies_metadata': f"{hdfs_base_path}/project/movies_dataset/movies_metadata.csv",
    'ratings': f"{hdfs_base_path}/project/movies_dataset/ratings.csv",
    'ratings_small': f"{hdfs_base_path}/project/movies_dataset/ratings_small.csv",
    'credits': f"{hdfs_base_path}/project/movies_dataset/credits.csv",
    'keywords': f"{hdfs_base_path}/project/movies_dataset/keywords.csv",
    'links': f"{hdfs_base_path}/project/movies_dataset/links.csv",
    'links_small': f"{hdfs_base_path}/project/movies_dataset/links_small.csv",

    # IMDB数据集
    'imdb_basics': f"{hdfs_base_path}/project/imdb_datasets/title.basics.tsv",
    'imdb_ratings': f"{hdfs_base_path}/project/imdb_datasets/title.ratings.tsv",
    'imdb_principals': f"{hdfs_base_path}/project/imdb_datasets/title.principals.tsv",
    'imdb_names': f"{hdfs_base_path}/project/imdb_datasets/name.basics.tsv",
    'imdb_akas': f"{hdfs_base_path}/project/imdb_datasets/title.akas.tsv",
    'imdb_episode': f"{hdfs_base_path}/project/imdb_datasets/title.episode.tsv",
    'imdb_crew': f"{hdfs_base_path}/project/imdb_datasets/title.crew.tsv",

    # 评论数据
    'imdb_reviews': f"{hdfs_base_path}/project/imdb_50k_reviews/IMDBDataset.csv",

    # 外部数据源
    'rotten_tomatoes_movies': f"{hdfs_base_path}/project/rotten_tomatoes_reviews/rotten_tomatoes_movies.csv",
    'rotten_tomatoes_reviews': f"{hdfs_base_path}/project/rotten_tomatoes_reviews/rotten_tomatoes_critic_reviews.csv",
    'netflix': f"{hdfs_base_path}/project/netflix_movies_tv_shows/netflix_titles.csv"
}

# -------------------------------------------------------------------
# 0) Spark 初始化
# -------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("BuildMoviesABT")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.execution.arrow.enabled", "true")
    .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
    .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext
sc.setLogLevel("WARN")  # 或者 "ERROR"

# -------------------------------------------------------------------
# 1) 读取 (Read)
# -------------------------------------------------------------------
# 所有文件均在当前路径
links_path = data_paths["links"]
movies_path = data_paths["movies_metadata"]
imdb_ratings_path = data_paths["imdb_ratings"]
rt_path = data_paths["rotten_tomatoes_movies"]
nf_path = data_paths["netflix"]
reviews_path = data_paths["imdb_reviews"]

links = (
    spark.read.option("header", True).csv(links_path)
)

# movies_metadata 可能包含换行与引号
movies = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("escape", "\"")
    .csv(movies_path)
)

imdb_ratings = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .csv(imdb_ratings_path)
)

credits_path = data_paths["credits"]
credits = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("escape", "\"")
    .csv(credits_path)
)


rt = spark.read.option("header", True).csv(rt_path)
nf = spark.read.option("header", True).csv(nf_path)

# IMDB 50K Reviews（可能只有 review/sentiment 两列）
reviews = spark.read.option("header", True).csv(reviews_path)

# -------------------------------------------------------------------
# 2) 转换 (清洗)
# -------------------------------------------------------------------
# 2.1 links：标准化 imdb_id（tt + 左补零至至少7位；更长则保留）
links_clean = (
    links
    .withColumn("movieId", F.col("movieId").cast("int"))
    .withColumn("tmdb_id", F.col("tmdbId").cast("int"))
    .withColumn("imdb_id_numeric", F.col("imdbId").cast("string"))
    .withColumn(
        "imdb_id",
        F.when(F.col("imdbId").isNotNull(),
               F.concat(F.lit("tt"), F.lpad(F.col("imdbId").cast("string"), 7, "0"))
        ).otherwise(F.lit(None).cast("string"))
    )
    .select("movieId", "tmdb_id", "imdb_id_numeric", "imdb_id")
    .dropDuplicates(["imdb_id"])
)

# 2.2 movies_metadata：强转数值、解析日期、解析 genres
#    - id 列可能有非数值，需过滤或安全转换
movies_clean = (
    movies
    .withColumn("id_int", F.when(F.col("id").rlike("^[0-9]+$"), F.col("id").cast("int")).otherwise(F.lit(None).cast("int")))
    .withColumn("budget_usd", F.col("budget").cast("long"))
    .withColumn("revenue_usd", F.col("revenue").cast("long"))
    .withColumn("runtime_minutes", F.col("runtime").cast("double"))
    .withColumn("popularity_tmdb", F.col("popularity").cast("double"))
    .withColumn("vote_average_tmdb", F.col("vote_average").cast("double"))
    .withColumn("vote_count_tmdb", F.col("vote_count").cast("int"))
    .withColumn("release_date_raw", F.col("release_date"))
    # 尝试多种日期格式
    .withColumn("release_date",
                F.coalesce(
                    F.to_date("release_date_raw", "yyyy-MM-dd"),
                    F.to_date("release_date_raw", "yyyy/MM/dd"),
                    F.to_date("release_date_raw", "MM/dd/yyyy"),
                    F.to_date("release_date_raw", "dd/MM/yyyy")
                ))
    .withColumn("release_year", F.year("release_date"))
    # genres 解析为数组<string>
    .withColumn(
        "genres_json",
        F.from_json(F.col("genres"), T.ArrayType(T.StructType([
            T.StructField("id", T.IntegerType(), True),
            T.StructField("name", T.StringType(), True),
        ])))
    ).withColumn("genres_tmdb", F.expr("transform(genres_json, x -> x.name)"))
    .select(
        F.col("imdb_id").alias("imdb_id_movies"),
        "id_int",
        F.col("title"),
        F.col("original_title"),
        "release_date",
        "release_year",
        "original_language",
        "genres_tmdb",
        "budget_usd",
        "revenue_usd",
        "runtime_minutes",
        F.col("status"),
        "popularity_tmdb",
        "vote_average_tmdb",
        "vote_count_tmdb"
    )
)

# 2.6 Credits数据清洗和解析
def extract_credits_direct(cast_str, crew_str):
    """优化的直接提取方法"""
    try:
        # 尝试使用ast.literal_eval安全解析
        cast_data = ast.literal_eval(cast_str) if cast_str and cast_str.strip() not in ['', '[]'] else []
        crew_data = ast.literal_eval(crew_str) if crew_str and crew_str.strip() not in ['', '[]'] else []

        # 提取演员信息（按order排序的前5名）
        names = []
        characters = []
        if isinstance(cast_data, list) and cast_data:
            # 过滤有效数据并按order排序
            valid_cast = [x for x in cast_data if isinstance(x, dict) and x.get('name')]
            sorted_cast = sorted(valid_cast, key=lambda x: x.get('order', 999))

            # 取前5个
            for actor in sorted_cast[:5]:
                name = str(actor.get('name', '')).strip()
                character = str(actor.get('character', '')).strip()
                if name:
                    names.append(name)
                    characters.append(character)

        # 提取导演信息
        directors = []
        if isinstance(crew_data, list) and crew_data:
            for person in crew_data:
                if (isinstance(person, dict) and
                        person.get('name') and
                        person.get('job', '').lower() == 'director'):
                    directors.append(str(person.get('name')).strip())

        return names, characters, directors, "eval"

    except:
        # 如果eval失败，使用正则表达式回退
        names_regex = []
        characters_regex = []
        directors_regex = []

        # 正则提取演员
        if cast_str:
            name_pattern = r"'name'\s*:\s*'([^']*)'"
            char_pattern = r"'character'\s*:\s*'([^']*)'"
            order_pattern = r"'order'\s*:\s*(\d+)"

            # 找到所有匹配项
            name_matches = re.findall(name_pattern, cast_str)
            char_matches = re.findall(char_pattern, cast_str)
            order_matches = re.findall(order_pattern, cast_str)

            # 组合并排序
            actors = []
            for i in range(min(len(name_matches), len(char_matches), len(order_matches))):
                try:
                    order = int(order_matches[i])
                    actors.append((order, name_matches[i], char_matches[i]))
                except:
                    continue

            # 按order排序并取前5个
            actors.sort(key=lambda x: x[0])
            for order, name, char in actors[:5]:
                if name.strip():
                    names_regex.append(name.strip())
                    characters_regex.append(char.strip())

        # 正则提取导演
        if crew_str:
            director_pattern = r"'job'\s*:\s*'Director'[^}]*'name'\s*:\s*'([^']*)'"
            directors_regex = re.findall(director_pattern, crew_str)

        return names_regex, characters_regex, directors_regex, "regex"


# 注册UDF
extract_credits_udf = F.udf(extract_credits_direct, T.StructType([
    T.StructField("names", T.ArrayType(T.StringType()), True),
    T.StructField("characters", T.ArrayType(T.StringType()), True),
    T.StructField("directors", T.ArrayType(T.StringType()), True),
    T.StructField("method", T.StringType(), True)
]))


def extract_additional_crew(crew_str):
    """提取编剧和制片人信息"""
    try:
        crew_data = ast.literal_eval(crew_str) if crew_str and crew_str.strip() not in ['', '[]'] else []

        writers = []
        producers = []

        if isinstance(crew_data, list) and crew_data:
            for person in crew_data:
                if isinstance(person, dict) and person.get('name'):
                    job = person.get('job', '').lower()
                    name = str(person.get('name')).strip()

                    if 'writer' in job or 'screenplay' in job:
                        writers.append(name)
                    elif 'producer' in job:
                        producers.append(name)

        return writers, producers

    except:
        # 正则回退
        writers_regex = []
        producers_regex = []

        if crew_str:
            # 提取编剧
            writer_pattern = r"'job'\s*:\s*'[^']*(?:Writer|Screenplay)[^']*'[^}]*'name'\s*:\s*'([^']*)'"
            writers_regex = re.findall(writer_pattern, crew_str, re.IGNORECASE)

            # 提取制片人
            producer_pattern = r"'job'\s*:\s*'[^']*Producer[^']*'[^}]*'name'\s*:\s*'([^']*)'"
            producers_regex = re.findall(producer_pattern, crew_str, re.IGNORECASE)

        return writers_regex, producers_regex


# 注册UDF
extract_crew_udf = F.udf(extract_additional_crew, T.StructType([
    T.StructField("writers", T.ArrayType(T.StringType()), True),
    T.StructField("producers", T.ArrayType(T.StringType()), True)
]))

credits_clean = (
    credits
    .withColumn("tmdb_id", F.col("id").cast("int"))
    # 提取主要信息
    .withColumn("extracted_credits", extract_credits_udf(F.col("cast"), F.col("crew")))
    .withColumn("extracted_crew", extract_crew_udf(F.col("crew")))
    # 展开字段
    .withColumn("main_cast_names", F.col("extracted_credits.names"))
    .withColumn("main_cast_characters", F.col("extracted_credits.characters"))
    .withColumn("director_names", F.col("extracted_credits.directors"))
    .withColumn("writer_names", F.col("extracted_crew.writers"))
    .withColumn("producer_names", F.col("extracted_crew.producers"))
    .withColumn("extract_method", F.col("extracted_credits.method"))
    # 统计信息
    .withColumn("has_cast", F.size(F.col("main_cast_names")) > 0)
    .withColumn("has_director", F.size(F.col("director_names")) > 0)
    .withColumn("has_writer", F.size(F.col("writer_names")) > 0)
    .withColumn("has_producer", F.size(F.col("producer_names")) > 0)
    .withColumn("cast_count", F.size(F.col("main_cast_names")))
    # 选择最终字段
    .select(
        "tmdb_id",
        "main_cast_names",
        "main_cast_characters",
        "director_names",
        "writer_names",
        "producer_names",
        "cast_count",
        "has_cast",
        "has_director",
        "has_writer",
        "has_producer",
        "extract_method"
    )
    .dropDuplicates(["tmdb_id"])
)

# 2.3 构造 ABT 基表（以 links 为主，左连接 movies）
abt_base = (
    links_clean.alias("l")
    .join(
        movies_clean.alias("m"),
        on=( (F.col("l.imdb_id") == F.col("m.imdb_id_movies")) | (F.col("l.tmdb_id") == F.col("m.id_int")) ),
        how="left"
    )
    .select(
        F.col("l.imdb_id"),
        F.col("l.imdb_id_numeric"),
        F.col("l.tmdb_id"),
        F.col("l.movieId"),
        F.col("m.title"),
        F.col("m.original_title"),
        F.col("m.release_date"),
        F.col("m.release_year"),
        F.col("m.original_language"),
        F.col("m.genres_tmdb"),
        F.col("m.budget_usd"),
        F.col("m.revenue_usd"),
        F.col("m.runtime_minutes"),
        F.col("m.status"),
        F.col("m.popularity_tmdb"),
        F.col("m.vote_average_tmdb"),
        F.col("m.vote_count_tmdb"),
    )
)

# 2.4 IMDb 评分清洗
imdb_ratings_clean = (
    imdb_ratings
    .select(
        F.col("tconst").alias("imdb_id"),
        F.col("averageRating").cast("double").alias("imdb_average_rating"),
        F.col("numVotes").cast("int").alias("imdb_num_votes")
    )
)

# 2.5 Rotten Tomatoes 清洗（字段名可能有差异，做兼容）
rt_clean = (
    rt
    .withColumn("rt_movie_title", F.col("movie_title"))
    .withColumn("rt_audience_status", F.col("audience_status"))
    .withColumn("rt_audience_count", F.col("audience_count").cast("int"))
    .withColumn("rt_tomatometer_rating", F.col("tomatometer_rating").cast("int"))
    .withColumn("rt_audience_rating", F.col("audience_rating").cast("int"))
    .withColumn("rt_release_raw",F.col("original_release_date"))
    .withColumn("rt_original_release_date",
                F.coalesce(
                    F.to_date("rt_release_raw", "yyyy-MM-dd"),
                    F.to_date("rt_release_raw", "yyyy/MM/dd"),
                    F.to_date("rt_release_raw", "MM/dd/yyyy"),
                    F.to_date("rt_release_raw", "dd/MM/yyyy")
                ))
    .withColumn("rt_year", F.year("rt_original_release_date"))
    .select("rt_movie_title", "rt_original_release_date", "rt_year", "rt_tomatometer_rating", "rt_audience_rating",
        "rt_audience_status",
        "rt_audience_count" )
    .dropna(subset=["rt_movie_title"])  # 标题缺失无法匹配
)

nf_clean = (
    nf
    .withColumn("nf_title", F.col("title"))
    .withColumn("nf_type", F.col("type"))
    .withColumn("nf_release_year", F.col("release_year").cast("int"))
    .withColumn("nf_date_added_raw", F.col("date_added"))
    .withColumn("nf_date_added",
                F.coalesce(
                    F.to_date("nf_date_added_raw", "MMMM d, yyyy"),
                    F.to_date("nf_date_added_raw", "MMM d, yyyy"),
                    F.to_date("nf_date_added_raw", "M/d/yyyy")
                ))
    .withColumn("nf_show_id", F.col("show_id"))
    .select("nf_title", "nf_release_year", "nf_type", "nf_show_id", "nf_date_added")
    .dropna(subset=["nf_title"])
)

# -------------------------------------------------------------------
# 3) 转换 (模糊匹配 UDF) —— 实际采用内置函数实现 Jaccard
# -------------------------------------------------------------------
# 文本标准化：小写、去标点、分词、去停用词、去重
#STOPWORDS = ["the","a","an","and","of","in","on","to","for","at","with","from","by","part","vol","episode","edition"]


def title_tokens(col):

    cleaned = F.regexp_replace(F.coalesce(col, F.lit("")), "[^a-zA-Z0-9]+", " ")
    cleaned = F.lower(F.trim(cleaned))

    # 分割单词
    tokens = F.split(cleaned, "\\s+")

    return F.array_distinct(tokens)

def jaccard_sim(a, b):
    inter = F.size(F.array_intersect(a, b))
    uni = F.size(F.array_union(a, b))
    return F.when(uni == 0, F.lit(0.0)).otherwise(inter.cast("double") / uni)

# 为 ABT、RT、NF 生成 tokens
abt_tok = (
    abt_base
    .withColumn("abt_title_tokens", title_tokens(F.col("title")))
    .cache()
)

rt_tok = (
    rt_clean
    .withColumn("rt_title_tokens", title_tokens(F.col("rt_movie_title")))
)

nf_tok = (
    nf_clean
    .withColumn("nf_title_tokens", title_tokens(F.col("nf_title")))
)

# 3.1 RT 模糊匹配：按年份窗口（±1年）构造候选，再算 Jaccard+年份得分，取每个 imdb_id 最优
cand_rt = (
    abt_tok.alias("a")
    .join(
        rt_tok.alias("r"),
        on=(
            (F.col("a.release_year").isNotNull()) &
            (F.col("r.rt_year").isNotNull()) &
            (F.abs(F.col("a.release_year") - F.col("r.rt_year")) <= 1)
        ),
        how="inner"
    )
    .withColumn("title_jaccard", jaccard_sim(F.col("a.abt_title_tokens"), F.col("r.rt_title_tokens")))
    .withColumn(
        "year_score",
        F.when(F.col("a.release_year").isNull() | F.col("r.rt_year").isNull(), F.lit(0.0))
         .when(F.col("a.release_year") == F.col("r.rt_year"), F.lit(1.0))
         .when(F.abs(F.col("a.release_year") - F.col("r.rt_year")) == 1, F.lit(0.7))
         .otherwise(F.lit(0.0))
    )
    .withColumn("rt_match_score", (F.col("title_jaccard") * 0.85 + F.col("year_score") * 0.15))
    .where(F.col("rt_match_score") >= 0.55)
)

w_rt = Window.partitionBy("a.imdb_id").orderBy(F.col("rt_match_score").desc_nulls_last())
rt_best = (
    cand_rt
    .withColumn("rn", F.row_number().over(w_rt))
    .where(F.col("rn") == 1)
    .where(F.col("rt_match_score") >= 0.7)  # 阈值可调
    .select(
        F.col("a.imdb_id").alias("imdb_id"),
        F.col("r.rt_movie_title").alias("rt_movie_title"),
        F.col("r.rt_original_release_date").alias("rt_original_release_date"),
        F.col("r.rt_tomatometer_rating").alias("rt_tomatometer_rating"),
        F.col("r.rt_audience_rating").alias("rt_audience_rating"),
        F.col("r.rt_audience_status").alias("rt_audience_status"),
        F.col("r.rt_audience_count").alias("rt_audience_count"),
        F.col("rt_match_score"),
        F.lit("jaccard_year_v1").alias("rt_match_method")
    )
)

# 3.2 Netflix 模糊匹配：年份窗口（±1年）+ Jaccard
cand_nf = (
    abt_tok.alias("a")
    .join(
        nf_tok.alias("n"),
        on=(
            (F.col("a.release_year").isNotNull()) &
            (F.col("n.nf_release_year").isNotNull()) &
            (F.abs(F.col("a.release_year") - F.col("n.nf_release_year")) <= 1)
        ),
        how="inner"  # 关键修改：改为内连接
    )
    .withColumn("title_jaccard", jaccard_sim(F.col("a.abt_title_tokens"), F.col("n.nf_title_tokens")))
    .withColumn(
        "year_score",
        F.when(F.col("a.release_year") == F.col("n.nf_release_year"), F.lit(1.0))
         .when(F.abs(F.col("a.release_year") - F.col("n.nf_release_year")) == 1, F.lit(0.7))
         .otherwise(F.lit(0.0))
    )
    .withColumn("nf_match_score", (F.col("title_jaccard") * 0.85 + F.col("year_score") * 0.15))
    .where(F.col("nf_match_score") >= 0.55)  # 提前过滤低分匹配
)

w_nf = Window.partitionBy("a.imdb_id").orderBy(F.col("nf_match_score").desc_nulls_last())
nf_best = (
    cand_nf
    .withColumn("rn", F.row_number().over(w_nf))
    .where(F.col("rn") == 1)
    .where(F.col("nf_match_score") >= 0.7)  # 阈值可调
    .select(
        F.col("a.imdb_id").alias("imdb_id"),
        F.lit(True).alias("nf_available"),
        F.col("n.nf_title").alias("nf_title"),
        F.col("n.nf_release_year").alias("nf_release_year"),
        F.col("n.nf_type").alias("nf_type"),
        F.col("n.nf_show_id").alias("nf_show_id"),
        F.col("n.nf_date_added").alias("nf_date_added"),
        F.col("nf_match_score"),
        F.lit("jaccard_year_v1").alias("nf_match_method")
    )
)

# -------------------------------------------------------------------
# 4) 加载 (Join)
# -------------------------------------------------------------------
abt = (
    abt_base.alias("a")
    .join(imdb_ratings_clean.alias("r"), on="imdb_id", how="left")
    .join(rt_best.alias("rt"), on="imdb_id", how="left")
    .join(nf_best.alias("nf"), on="imdb_id", how="left")
    .join(credits_clean.alias("c"), on="tmdb_id", how="left")
)

# -------------------------------------------------------------------
# 5) 文本评论聚合（IMDB 50K Reviews）
#    若 reviews 含 imdb_id 列：按 imdb_id 聚合；否则产生空聚合（列均为 NULL）
# -------------------------------------------------------------------
if "imdb_id" in [c.lower() for c in reviews.columns]:
    # 规范列名（兼容不同大小写）
    cols_map = {c.lower(): c for c in reviews.columns}
    imdb_id_col = cols_map.get("imdb_id")
    review_col = cols_map.get("review", "review")
    sentiment_col = cols_map.get("sentiment", "sentiment")

    reviews_aug = (
        reviews
        .withColumnRenamed(imdb_id_col, "imdb_id")
        .withColumnRenamed(review_col, "review")
        .withColumnRenamed(sentiment_col, "sentiment")
        .withColumn("rev_len", F.length(F.col("review")))
        .withColumn("is_pos", F.when(F.lower(F.col("sentiment")) == F.lit("positive"), F.lit(1.0)).otherwise(F.lit(0.0)))
        .select("imdb_id", "rev_len", "is_pos")
    )

    rev_agg = (
        reviews_aug.groupBy("imdb_id")
        .agg(
            F.count(F.lit(1)).cast("int").alias("rev_count"),
            F.avg("is_pos").alias("rev_pos_share"),
            F.avg("rev_len").alias("rev_avg_len")
        )
    )
else:
    # 构造空表（与 abt 主键连接后得到 NULL）
    rev_schema = T.StructType([
        T.StructField("imdb_id", T.StringType(), True),
        T.StructField("rev_count", T.IntegerType(), True),
        T.StructField("rev_pos_share", T.DoubleType(), True),
        T.StructField("rev_avg_len", T.DoubleType(), True),
    ])
    rev_agg = spark.createDataFrame([], schema=rev_schema)

abt = abt.join(rev_agg, on="imdb_id", how="left")

# -------------------------------------------------------------------
# 6) 选列与整理输出 Schema（保持清晰的前缀）
# -------------------------------------------------------------------
# 若没有命中的 Netflix，nf_available 为 False
abt = abt.withColumn("nf_available", F.coalesce(F.col("nf_available"), F.lit(False)))

final_cols = [
    # 主键与溯源
    "imdb_id", "imdb_id_numeric", "tmdb_id", "movieId",
    # 元数据
    "title","original_title","release_date","release_year","original_language",
    "genres_tmdb","budget_usd","revenue_usd","runtime_minutes","status",
    "popularity_tmdb","vote_average_tmdb","vote_count_tmdb",
    # IMDb
    "imdb_average_rating","imdb_num_votes",
    # Rotten Tomatoes（注意：无 rt. 前缀）
    "rt_movie_title","rt_original_release_date",
    "rt_tomatometer_rating","rt_audience_rating",
    "rt_audience_status","rt_audience_count",
    "rt_match_score","rt_match_method",
    # Netflix（注意：无 nf. 前缀）
    "nf_available","nf_title","nf_release_year","nf_type",
    "nf_show_id","nf_date_added","nf_match_score","nf_match_method",
    # 文本评论聚合
    "rev_count","rev_pos_share","rev_avg_len",
    "main_cast_names", "main_cast_characters",
    "director_names", "writer_names", "producer_names",
]

# 注意：final_cols 中既有字符串也有 Column，需要统一处理
select_exprs = [c if isinstance(c, F.Column) else F.col(c) for c in final_cols if c is not None]
abt_final = abt.select(*select_exprs)

# -------------------------------------------------------------------
# 7) 持久化写出 (Parquet)
# -------------------------------------------------------------------
out_path = "/project/result1/abt_movies.parquet"
abt_final.write.mode("overwrite").parquet(out_path)

# 可选：打印 Schema 与样例
print("===== ABT Schema =====")
abt_final.printSchema()
print("===== ABT Sample =====")
for row in abt_final.limit(5).collect():
    print(row)

spark.stop()

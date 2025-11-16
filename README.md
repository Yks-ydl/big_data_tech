# big_data_tech
A project that uses big data technology to analyze the divergence between IMDB movie reviews, professional film critics' ratings, and ordinary audiences' ratings.

## Spark Analyse
### Docker Image Building & Running 
Run the command below to build the image below
```
cd spark_analyze && docker build . -t spark-analyze
```
Run the spark image in detached mode with an local folder C:\Users\17314\OneDrive\Desktop\share_folder:/data mounted on the container. We can easily transfer files through this shared folder from host machine to the container.
```
docker run -it -d --name spark-bigdata --rm -v C:\Users\17314\OneDrive\Desktop\share_folder:/data spark-bigdata:latest /bin/bash
```

Login to the container with a bash shell so that we can execute command in the container directly.
```
docker exec -it   spark-bigdata /bin/bash
```

Submit the Classify job, which is using kmeans algorithm, to the spark in the container
```
spark-shell -i MovieClassificationKMeans.scala
```
It will processed all the parquet in /data/movie_parquets/ which is produced by main.py. The main function of this procedure is to classify the parquets in four categories

Then run word_freq_count.scala, it will statisticize the word frequency.

```
spark-shell -i word_freq_count.scala
```


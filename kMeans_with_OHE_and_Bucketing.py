import pyspark
import csv
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import col, column
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql.functions import array_contains
from pyspark.sql import Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, IndexToString, PCA
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import pandas as pd
import numpy as np
import math


ss = SparkSession.builder.appName("K_means_w/_OHE_&_Bucketing").getOrCreate()

Scanners_big_df = ss.read.csv("./Day_2020_profile.csv", header=True, inferSchema=True )

Scanners_big_df.printSchema()
Scanners_big_df.select("Packets").describe().show()


Scanners_df = ss.read.csv("./Day_2020_profile.csv", header= True, inferSchema=True )
Scanners_df.printSchema()


Scanners_df.stat.corr("Packets", "Bytes")


from pyspark.sql.functions import corr
Scanners_df.select(corr("Packets", "Bytes")).show()

Scanners_df.select(corr("lifetime", "Packets")).show()
Scanners_df.select(corr("lifetime", "numports")).show()

Scanners_df.select(corr("numports", "MinUniqueDests")).show()

Scanners_df.select("Packets").describe().show()

Packets_RDD=Scanners_df.select("Packets").rdd
Packets_rdd = Packets_RDD.map(lambda row: row[0])

Packets_rdd.histogram([0,10,100,1000,10000,100000,1000000,10000000,100000000])

from pyspark.ml.feature import Bucketizer
bucketBorders=[-1.0, 5.0, 10.0, 50.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0 , 100000000.0]
bucketer = Bucketizer().setSplits(bucketBorders).setInputCol("Packets").setOutputCol("Packets_B10")
Scanners2_df = bucketer.transform(Scanners_df)

Scanners2_df.printSchema()
Scanners2_df.select("Packets","Packets_B10").where("Packets > 100000").show(30)
Scanners2_df.select("numports").describe().show()
Scanners_df.where(col('mirai')).count()
Scanners_df.select("ports_scanned_str").show(30)

Scanners3_df=Scanners2_df.withColumn("Ports_Array", split(col("ports_scanned_str"), "-") )
Scanners_df2.persist().show(10)

Ports_Scanned_RDD = Scanners3_df.select("Ports_Array").rdd
Ports_Scanned_RDD.persist().take(5)

Ports_list_RDD = Ports_Scanned_RDD.map(lambda row: row[0] )
Ports_list_RDD.persist()

Ports_list2_RDD = Ports_Scanned_RDD.flatMap(lambda row: row[0] )

Port_count_RDD = Ports_list2_RDD.map(lambda x: (x, 1))
Port_count_RDD.take(2)

Port_count_total_RDD = Port_count_RDD.reduceByKey(lambda x,y: x+y, 1)
Port_count_total_RDD.persist().take(5)

Sorted_Count_Port_RDD = Port_count_total_RDD.map(lambda x: (x[1], x[0])).sortByKey( ascending = False)
Sorted_Count_Port_RDD.persist().take(50)

top_ports= 50
Sorted_Ports_RDD= Sorted_Count_Port_RDD.map(lambda x: x[1])
Top_Ports_list = Sorted_Ports_RDD.take(top_ports)
Top_Ports_list

Scanners_df3=Scanners_df2.withColumn(FeatureName, array_contains("Ports_Array", Top_Ports_list[0]))
Scanners_df3.show(10)


for i in range(0, top_ports):
    # "Port" + Top_Ports_list[i]  is the name of each new feature created through One Hot Encoding
    Scanners_df3 = Scanners3_df.withColumn("Port" + Top_Ports_list[i], array_contains("Ports_Array", Top_Ports_list[i]))
    Scanners3_df = Scanners_df3

Scanners3_df.printSchema()


cluster_num = 100
seed = 123
km = KMeans(featuresCol="features", predictionCol="prediction").setK(cluster_num).setSeed(seed)
km.explainParams()


input_features = ["Packets_B10"]
for i in range(0, top_ports):
    input_features.append( "Port"+Top_Ports_list[i] )

print(input_features)

va = VectorAssembler().setInputCols(input_features).setOutputCol("features")

data= va.transform(Scanners3_df)

data.persist()


kmModel=km.fit(data)


kmModel

predictions = kmModel.transform(data)
predictions.persist()

Cluster0_df=predictions.where(col("prediction")==0)
Cluster0_df.count()

summary = kmModel.summary

summary.clusterSizes

evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print('Silhouette Score of the Clustering Result is ', silhouette)

centers = kmModel.clusterCenters()
centers[0]

print("Cluster Centers:")
i=0
for center in centers:
    print("Cluster ", str(i+1), center)
    i = i+1
    
len(input_features)

centers[0]
centers[0][50]

column_list = ['cluster ID', 'size', 'mirai_ratio' ]
for feature in input_features:
    column_list.append(feature)
mirai_clusters_df = pd.DataFrame( columns = column_list )
threshold = 0.2
for i in range(0, top_ports):
    cluster_row = [ ]
    cluster_i = predictions.where(col('prediction')==i)
    cluster_i_size = cluster_i.count()
    cluster_i_mirai_count = cluster_i.where(col('mirai')).count()
    cluster_i_mirai_ratio = cluster_i_mirai_count/cluster_i_size
    if cluster_i_mirai_count > 0:
        print("Cluster ", i, "; Mirai Ratio:", cluster_i_mirai_ratio, "; Cluster Size: ", cluster_i_size)
    if cluster_i_mirai_ratio > threshold:
        cluster_row = [i, cluster_i_size, cluster_i_mirai_ratio]
        # Add the cluster center (average) value for each input feature for cluster i to cluster_row
        for j in range(0, len(input_features)):
            cluster_row.append(centers[i][j])
        mirai_clusters_df.loc[i]= cluster_row


mirai_clusters_df.to_csv("./Bucketing10_cluster.csv")


input_features2 = [ ]
for i in range(0, top_ports ):
    input_features2.append( "Port"+Top_Ports_list[i] )

print(input_features2)

va2 = VectorAssembler().setInputCols(input_features2).setOutputCol("features2")

data2= va2.transform(Scanners3_df)

data2.persist()


km2 = KMeans(featuresCol="features2", predictionCol="prediction2").setK(cluster_num).setSeed(seed)
km2.explainParams()

kmModel2=km2.fit(data2)
kmModel2

predictions2 = kmModel2.transform(data2)

predictions2.persist()

summary2 = kmModel2.summary
summary2.clusterSizes


centers2 = kmModel2.clusterCenters()

evaluator2 = ClusteringEvaluator(featuresCol='features2', predictionCol='prediction2')
silhouette2 = evaluator2.evaluate(predictions2)

print('Silhouette Score of the Clustering Result is ', silhouette2)

input_features2


column_list2 = ['cluster ID', 'size', 'mirai_ratio' ]
for feature in input_features2:
    column_list2.append(feature)
mirai_clusters2_df = pd.DataFrame( columns = column_list2 )
threshold = 0.2
for i in range(0, top_ports):
    cluster_i = predictions2.where(col('prediction2')==i)
    cluster_i_size = cluster_i.count()
    cluster_i_mirai_count = cluster_i.where(col('mirai')).count()
    cluster_i_mirai_ratio = cluster_i_mirai_count/cluster_i_size
    if cluster_i_mirai_count > 0:
        print("Cluster ", i, "; Mirai Ratio:", cluster_i_mirai_ratio, "; Cluster Size: ", cluster_i_size)
    if cluster_i_mirai_ratio > threshold:
        cluster_row2 = [i, cluster_i_size, cluster_i_mirai_ratio]
        for j in range(0, len(input_features2)):
            cluster_row2.append(centers2[i][j])
        mirai_clusters2_df.loc[i]= cluster_row2


mirai_clusters2_df.to_csv("./OHE_cluster.csv")


# Distribtion of scanner's country in a cluster.

cluster = predictions2.where(col('prediction2')==12)

cluster.groupBy("Country").count().orderBy("count", ascending=False).show()


ss.stop()






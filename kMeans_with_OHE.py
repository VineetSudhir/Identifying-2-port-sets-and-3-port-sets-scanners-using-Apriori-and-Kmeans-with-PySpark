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
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, IndexToString
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

ss = SparkSession.builder.appName("Clustering OHE").getOrCreate()

Scanners_df = ss.read.csv("./Day_2020_profile.csv", header= True, inferSchema=True )
Scanners_df.printSchema()

Scanners_df.select("ports_scanned_str").show(4)
Scanners_df2=Scanners_df.withColumn("Ports_Array", split(col("ports_scanned_str"), "-") )
Scanners_df2.show(10)

Ports_Scanned_RDD = Scanners_df2.select("Ports_Array").rdd

Ports_list_RDD = Ports_Scanned_RDD.map(lambda row: row[0] )

Ports_list_RDD.take(3)

Ports_list2_RDD = Ports_Scanned_RDD.flatMap(lambda row: row[0] )

Ports_list2_RDD.take(7)

Port_count_RDD = Ports_list2_RDD.map(lambda x: (x, 1))
Port_count_RDD.take(7)

Port_count_total_RDD = Port_count_RDD.reduceByKey(lambda x,y: x+y)
Port_count_total_RDD.take(5)

Port_count_total_RDD.count()

Sorted_Count_Port_RDD = Port_count_total_RDD.map(lambda x: (x[1], x[0])).sortByKey( ascending = False)

Sorted_Count_Port_RDD.take(50)

top_ports= 50
Sorted_Ports_RDD= Sorted_Count_Port_RDD.map(lambda x: x[1] )
Top_Ports_list = Sorted_Ports_RDD.take(top_ports)

Top_Ports_list

FeatureName = "Port"+Top_Ports_list[0]
FeatureName

from pyspark.sql.functions import array_contains

Scanners_df3=Scanners_df2.withColumn(FeatureName, array_contains("Ports_Array", Top_Ports_list[0]))
Scanners_df3.show(10)

First_top_port_scanners_count = Scanners_df3.where(col("Port17132") == True).rdd.count()

print(First_top_port_scanners_count)
top_ports

Top_Ports_list[49]

for i in range(0, top_ports):
    # "Port" + Top_Ports_list[i]  is the name of each new feature created through One Hot Encoding
    Scanners_df3 = Scanners_df2.withColumn("Port" + (Top_Ports_list[i]), array_contains("Ports_Array", Top_Ports_list[i]))
    Scanners_df2 = Scanners_df3

Scanners_df2.printSchema()

input_features = [ "lifetime", "Packets"]
for i in range(0, top_ports ):
    input_features.append( "Port"+ Top_Ports_list[i] )


print(input_features)


va = VectorAssembler().setInputCols(input_features).setOutputCol("features")
data= va.transform(Scanners_df2)

data.show(3)
data.persist()

km = KMeans(featuresCol= "features", predictionCol="prediction").setK(100).setSeed(123)
km.explainParams()

kmModel=km.fit(data)
kmModel

predictions = kmModel.transform(data)

predictions.show(3)
Cluster1_df=predictions.where(col("prediction")==0)

Cluster1_df.count()
summary = kmModel.summary

summary.clusterSizes

evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)

print('Silhouette Score of the Clustering Result is ', silhouette)

centers = kmModel.clusterCenters()

sc = ss.sparkContext

centers_rdd = sc.parallelize(centers)
centers_rdd.saveAsTextFile("./OHE_numerical")

input_features2 = [ ]
for i in range(0, top_ports):
    input_features2.append( "Port"+ Top_Ports_list[i] )

print(input_features2)

va2 = VectorAssembler().setInputCols(input_features2).setOutputCol("features2")
data2= va2.transform(Scanners_df2)
data2.persist()
data2.show(3)

km2 = KMeans(featuresCol="features2", predictionCol="prediction2").setK(100).setSeed(123)
km2.explainParams()


kmModel2=km2.fit(data2)
kmModel2

predictions2 = kmModel2.transform(data2)
predictions2.persist()


summary2 = kmModel2.summary
summary2.clusterSizes


evaluator2 = ClusteringEvaluator(featuresCol='features2', predictionCol='prediction2')
silhouette2 = evaluator2.evaluate(predictions2)

print('Silhouette Score of the Clustering Result is ', silhouette2)

centers2 = kmModel2.clusterCenters()
centers2_rdd = sc.parallelize(centers2)
centers2_rdd.saveAsTextFile("MiniProject 2 Cluster_Only_OHE")




ss.stop()


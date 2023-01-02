import pyspark
import csv
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import col, column
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql import Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, IndexToString
from pyspark.ml.clustering import KMeans


ss = SparkSession.builder.appName("Freqent Port Sets").getOrCreate()

Scanners_df = ss.read.csv("./Day_2020_profile.csv", header=True, inferSchema=True)

Scanners_df.printSchema()

# Want to split ports connected by dash. Example: "81-161-2000" is not one port, it is 3 individual ports, so I want to split it into an array of ports.

Scanners_df.select("ports_scanned_str")
Scanners_df2=Scanners_df.withColumn("Ports_Array", split(col("ports_scanned_str"), "-") )

Scanners_df2.select("ports_scanned_str","Ports_Array").rdd

Ports_Scanned_RDD = Scanners_df2.select("Ports_Array").rdd
Ports_Scanned_RDD.take(5)

multi_Ports_list_RDD = Ports_Scanned_RDD.map(lambda x: x[0])
multi_Ports_list_RDD.take(5)


# `flatMap` flatten the RDD into a list of ports. Then just count the occurance of each port in the RDD, which is the number of scanners that scan the port.   

port_list_RDD = multi_Ports_list_RDD.flatMap(lambda x: x)

Port_count_RDD = port_list_RDD.map(lambda x: (x,1) )
Port_count_RDD.take(5)


Port_count_total_RDD = Port_count_RDD.reduceByKey(lambda x,y: x+y, 1)
Port_count_total_RDD.take(5)

Port_count_total_RDD.count()

Sorted_Count_Port_RDD = Port_count_total_RDD.map(lambda x: (x[1], x[0])).sortByKey( ascending = False)
Sorted_Count_Port_RDD.take(10)


threshold = 4999
Filtered_Sorted_Count_Port_RDD= Sorted_Count_Port_RDD.filter(lambda x: x[0] > threshold)
Filtered_Sorted_Count_Port_RDD

Filtered_Sorted_Count_Port_RDD.count()


Top_Ports = Filtered_Sorted_Count_Port_RDD.map(lambda x: x[1]).collect()

Top_1_Port_count = len(Top_Ports)

Two_Port_Sets_df = pd.DataFrame( columns= ( ['Two Port Sets', 'count'] ))

print(Top_Ports)
print(Top_1_Port_count)


# Finding Frequent 2-Port Sets and 3-Port Sets

Three_Port_Sets_df = pd.DataFrame( columns= ['Port Sets', 'count'])
index_3 = 0
index_2 = 0
threshold = 4999
for i in range(0, Top_1_Port_count-1):
    Scanners_port_i_RDD = multi_Ports_list_RDD.filter(lambda x: Top_Ports[i] in x)
    Scanners_port_i_RDD.persist()  
    for j in range(i+1, Top_1_Port_count-1):
        Scanners_port_i_j_RDD = Scanners_port_i_RDD.filter(lambda x: Top_Ports[j] in x)
        Scanners_port_i_j_RDD.persist()
        two_ports_count = Scanners_port_i_j_RDD.count()
        if two_ports_count > threshold:
            Two_Port_Sets_df.loc[index_2] = [[Top_Ports[i], Top_Ports[j]], two_ports_count]
            index_2 = index_2 +1
            for k in range(j+1, Top_1_Port_count -1):
                Scanners_port_i_j_k_RDD = Scanners_port_i_j_RDD.filter(lambda x: Top_Ports[k] in x)
                three_ports_count = Scanners_port_i_j_k_RDD.count()
                if three_ports_count > threshold:
                    Three_Port_Sets_df.loc[index_3] = [ [Top_Ports[i], Top_Ports[j], Top_Ports[k]], three_ports_count]
                    index_3 = index_3 + 1
                    print("Three Ports: ", Top_Ports[i], ", ", Top_Ports[j], ",  ", Top_Ports[k], ": Count ", three_ports_count)
        Scanners_port_i_j_RDD.unpersist()
    Scanners_port_i_RDD.unpersist()


# Convert the Pandas dataframes into PySpark DataFrame

Two_Port_Sets_DF = ss.createDataFrame(Two_Port_Sets_df)
Three_Port_Sets_DF = ss.createDataFrame(Three_Port_Sets_df)


output_path_2_port = "./2Ports_cluster"
output_path_3_port = "./3Ports_cluster"
Two_Port_Sets_DF.rdd.saveAsTextFile(output_path_2_port)
Three_Port_Sets_DF.rdd.saveAsTextFile(output_path_3_port)

ss.stop()






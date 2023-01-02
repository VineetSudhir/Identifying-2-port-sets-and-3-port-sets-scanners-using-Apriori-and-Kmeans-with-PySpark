# Identifying-2-port-sets-and-3-port-sets-scanners-using-Apriori-and-Kmeans-with-PySpark


Utilized Apriori algorithm to identify frequent 2 port sets and 3 port sets that are scanned by scanners in the Darknet Big Data dataset.
Improved performance of frequent port set mining by the suitable reuse of RDD with persist and unpersist on the reused RDD.
Identify the set of top k ports for one-hot encoding ports scanned.
Employed K-means clustering, achieved a Silhouette score of: 0.9083484568569015.


Then applied bucketing to numerical variables for the integration with one-hot encoding features.
Employed K-means clustering by combining bucketing and one-hot encoding using the big data in cluster mode.
Utilized external label, mirai, to evaluate and compare the results of K-means clustering.

# Darknet Scanner Detection using Apriori and K-Means (PySpark)

This project analyzes **network scanner behavior** in a Darknet big data dataset by identifying frequently scanned port sets and clustering scanners using **PySpark**, **Apriori**, and **K-Means**. It combines **association rule mining** and **unsupervised learning** to identify patterns and anomalies — including scanning behaviors linked to **Mirai botnet** activity.

---

## 🔍 Project Overview

- **Data Source:** Darknet scanner activity logs (`Day_2020_profile.csv`)
- **Tech Stack:** PySpark (RDDs + MLlib), Pandas, KMeans, Apriori-style mining, One-Hot Encoding, Bucketing
- **Goal:** Identify frequent port sets and cluster scanning behaviors to reveal suspicious/malicious patterns

---

## ⚙️ Core Techniques

### 🔹 Apriori-based Frequent Port Set Mining
- Extracted frequent **2-port and 3-port sets** from large scanner datasets
- Optimized performance using `RDD.persist()` and `unpersist()` to reduce recomputation overhead
- Filtered top-k frequent ports based on scanning count thresholds

### 🔹 K-Means Clustering with Feature Engineering
- Created One-Hot Encoded (OHE) features from most common scanned ports
- Applied **Bucketing** to high-variance numeric features (e.g., packet count)
- Combined OHE + Bucketed features to form a robust clustering vector
- Evaluated clustering with **Silhouette Score**: `0.908`

### 🔹 Botnet Label Integration
- Used the **Mirai label** as an external indicator to evaluate clusters
- Identified clusters with **high Mirai ratios**, showing correlation between port scanning behavior and botnet presence

---

## 📈 Results

- Identified frequent port sets like: `[23, 2323]`, `[80, 8080]`, `[22, 445, 3389]`
- Achieved high Silhouette Score (≈ 0.91) indicating well-separated clusters
- Discovered clusters where **>20% of scanners were labeled as Mirai**, suggesting clear behavioral traits
- Saved clustering results to CSV for further inspection

---

## 🗂️ Project Structure


---

## 📊 Sample Outputs

| Cluster | Size | Mirai Ratio | Notable Ports       |
|---------|------|-------------|---------------------|
| 4       | 1500 | 0.32        | 23, 2323            |
| 12      | 980  | 0.25        | 80, 443, 8080       |
| 27      | 2100 | 0.29        | 22, 445, 3389       |

---

## 🚀 Why This Project Matters

This project reflects **real-world cybersecurity analysis** using scalable big data tools:
- Combines **port set analysis (Apriori logic)** with **machine learning**
- Shows end-to-end **feature engineering** in PySpark
- Applies clustering to identify **malicious behavior patterns** like Mirai
- Demonstrates Spark proficiency, not just for analytics, but for behavior modeling

---

## 🛠️ Tools & Libraries

- PySpark (RDDs, MLlib, Structured API)
- Pandas (evaluation + export)
- ClusteringEvaluator (Silhouette Score)
- Spark Feature Transformers: VectorAssembler, StringIndexer, Bucketizer, OneHotEncoder

---

## ✅ Next Steps (Future Improvements)

- Automate hyperparameter tuning (e.g., number of clusters)
- Apply DBSCAN for non-linear separation
- Extend to time-series behavior modeling
- Add visualizations (cluster heatmaps or decision trees)

---

## 📬 Contact

Made by **Vineet Sudhir** — feel free to connect on [LinkedIn](#) or explore more on [GitHub](#)


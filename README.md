# **TimeStream - A Versioned Data Lakehouse with Atomic ETL Pipelines**  

🚀 **Data Versioning | Time Travel | Git-Like Branching | Atomic ETL**  

**TimeStream** is a **modern data lakehouse** designed for **data versioning, reproducibility, and atomic ETL pipelines**. It combines **Git-like data management** with **structured storage** to enable **branching, merging, and time travel** for analytics and AI applications.

With **TimeStream**, data engineers can work in isolated development branches, validate transformations, and merge updates seamlessly into production—ensuring **data consistency, efficiency, and scalability**.

---

## **Key Features**  
✅ **Git-Inspired Data Management** – Use branches and commits for tracking data changes over time.  
✅ **Atomic ETL Pipelines** – Guarantee **data consistency** by merging only validated changes.  
✅ **Time Travel & Auditing** – Query **historical snapshots** using commit hashes or timestamps.  
✅ **Efficient Storage** – Symbolic branching **reduces data duplication** and optimizes storage.  
✅ **Dockerized Deployment** – Fully **containerized stack**, easy to set up and run.  

---

## **Tech Stack**
| Technology  | Purpose |
|-------------|---------|
| **MinIO**   | S3-compatible object storage for data lake storage. |
| **Apache Iceberg** | Table format enabling time travel, snapshotting, and schema evolution. |
| **Nessie**  | Git-like catalog for data versioning, branching, and merging. |
| **Apache Spark** | Distributed computing engine for ETL and transformations. |
| **Jupyter Notebooks** | Interactive exploration and visualization of datasets. |

---

## **Dataset**
We use the **NYC Taxi Trip Data**, a real-world dataset from the [New York City Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).  

### **Dataset Overview**
- **Source:** Public dataset containing **trip details**, including pickup/drop-off locations, timestamps, fares, and passenger counts.
- **Format:** **Parquet**
- **Size:** **~1GB per month of data**  
- **Use Case:** Used for demonstrating **ETL processes**, **branch-based transformations**, and **time travel queries**.

---

## **Workflow: Versioned ETL Pipeline**
TimeStream follows a structured, **branch-based** workflow for data processing, similar to a Git workflow:

### **1️⃣ Raw Data Ingestion (`raw` branch)**
- Ingest raw data into **MinIO** object storage.
- Store **initial unprocessed data** in Apache Iceberg under a separate branch.

### **2️⃣ Data Transformation (`dev` branch)**
- Run transformations using **Apache Spark**.
- Perform **data cleaning**, **aggregations**, and **format conversions**.
- Store **intermediate results** in a separate development branch.

### **3️⃣ Validation & Quality Checks (`dev` branch)**
- Run automated **validation checks** to ensure data quality.
- Verify **schema correctness, missing values, and logical consistency**.

### **4️⃣ Promotion to Production (`main` branch)**
- Merge **validated** changes from `dev` into `main`.
- Ensure **atomic updates**, guaranteeing a consistent view of the data for consumers.

### **5️⃣ Time Travel & Auditing (Commit Hashes & Tags)**
- Retrieve **historical snapshots** using **Nessie commit hashes**.
- Query **previous states** of the data for **auditing and debugging**.

---

## **Getting Started**
Follow these **step-by-step instructions** to set up and run TimeStream.

### **1️⃣ Clone the Repository**
```sh
git clone https://github.com/muratkars/TimeStream.git
cd TimeStream
```

### **2️⃣ Start the Environment**
Use Docker Compose to start the services:
```sh
docker-compose up -d
```
This will spin up MinIO, Nessie, Apache Spark, Iceberg REST, and Jupyter Notebooks.

### **3️⃣ Explore the Data**
Open Jupyter Notebooks and explore the data:
```sh
jupyter notebook
```

### **4️⃣ Verify Services**
Run the following command to confirm everything is running:
```sh
docker ps
```
Ensure the following services are running:
- minio
- nessie
- spark-iceberg
- iceberg-rest
- mc
- jupyter

### **5️⃣ Run ETL Pipelines**  
Step 1: Ingest Data

Download and store raw data in MinIO:

```sh
python etl/ingest.py
```

Step 2: Transform Data

Process and clean data using Apache Spark: 

```sh
python etl/transform.py
```

Step 3: Validate & Merge Data
Validate the cleaned dataset before merging to main:    

```sh
python etl/validate.py
```

### **6️⃣ Explore Data in Jupyter**   
Launch Jupyter Notebook to query and explore datasets:

```sh
Open http://localhost:8888 and navigate to `notebooks/exploration.ipynb`
```

### **7️⃣ Query Data in Jupyter** 

```sh
df = spark.read.format("iceberg").load("nessie.timestream.cleaned_trips")
df.show(5)

```

To retrieve previous versions:

```sh
snapshot_id = "your_snapshot_id_here"
df_old = spark.read.format("iceberg").option("snapshot-id", snapshot_id).load("nessie.timestream.cleaned_trips")
df_old.show(5)
```


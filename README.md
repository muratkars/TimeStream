# **TimeStream - A Git-Inspired Versioned Data Lakehouse**  
🚀 **Atomic ETL Pipelines | Git-Like Data Versioning | Time Travel & Branching**  

TimeStream is a **versioned data lakehouse** built with **Apache Iceberg, Nessie, and MinIO**, enabling **Git-like branching, atomic updates, and time travel** for structured data. It provides a **scalable, reproducible, and efficient data workflow** for modern analytics and AI-driven applications.

## **Key Features**
✅ **Git-Inspired Data Management** – Use branches and commits for data evolution.  
✅ **Atomic ETL Pipelines** – Ensure consistency with isolated transformations before merging to production.  
✅ **Time Travel & Auditing** – Query historical snapshots with commit hashes or timestamps.  
✅ **Efficient Storage** – Symbolic branching reduces duplication, optimizing storage.  
✅ **Dockerized Deployment** – Easily spin up services with `docker-compose`.  

## **Tech Stack**
- **MinIO** → S3-compatible object storage  
- **Apache Iceberg** → Table format with schema evolution & snapshotting  
- **Nessie** → Git-like catalog for data versioning  
- **Apache Spark** → Distributed computing for ETL  
- **Jupyter Notebooks** → Interactive exploration  

## **Workflow**
1️⃣ **Raw Data Ingestion** → **`raw` branch**  
2️⃣ **Data Transformation** → **`dev` branch**  
3️⃣ **Validation & Quality Checks** → **`dev` branch**  
4️⃣ **Promotion to Production** → **`main` branch**  
5️⃣ **Time Travel** → Use **tags & commit hashes** for data lineage  

🔹 **Stable reads from `main`** | 🔹 **Engineers work in `dev` branches** | 🔹 **Atomic commits ensure data integrity**  

## **Quick Start**
Clone the repository and start the environment with Docker:
```sh
git clone https://github.com/muratkars/TimeStream.git
cd TimeStream
docker-compose up -d

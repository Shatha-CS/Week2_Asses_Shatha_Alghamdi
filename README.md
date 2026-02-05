# Week 2 Project — ETL & EDA Pipeline

## Project Overview
This project focuses on building a complete **ETL (Extract, Transform, Load) pipeline** and applying **Exploratory Data Analysis (EDA)** on the processed data.

The main objective is to apply ETL concepts in a practical, end-to-end workflow—starting from raw data ingestion, through data cleaning and transformation, and ending with analytical exploration and visualization.

This repository represents a full week of work, from handling raw datasets to delivering a clean, reproducible, and ready-to-run data project.

---

## Project Concept
A structured ETL pipeline was designed and implemented with the following stages:

- Loading raw data
- Data cleaning and validation
- Safe and consistent table joins
- Outlier handling using winsorization
- Generating a final analytics-ready table
- Logging execution details into a metadata file

The resulting processed data is then used to perform **EDA**, generating insights and visualizations that describe the overall behavior of the data.

---

## Project Setup & Execution

### 1. Navigate to the project root directory

### 2. Create a virtual environment
```bash
python -m venv .venv
```
### 3. Activate the Virtual Environment

**Windows**
```bash
.venv\Scripts\activate
```
### 4. Install Required Dependencies
```bash
pip install -r requirements.txt
```
## Running the ETL Pipeline

The ETL pipeline can be executed using:
```bash
python scripts/run_etl.py
```

## ETL Outputs

After running the ETL pipeline, the following files will be generated:

- `data/processed/analytics_table.parquet`  
  Final analytics table used for all downstream analysis

- `data/processed/orders_clean.parquet`  
  Cleaned version of the orders dataset

- `data/processed/users.parquet`  
  Cleaned version of the users dataset

- `data/processed/_run_meta.json`  
  Execution metadata including:
  - Number of output rows  
  - Join match rate  
  - Missing values statistics  
  - Execution timestamp and file paths  

---

## Exploratory Data Analysis (EDA)

EDA is performed using the notebook:
```bash
notebooks/eda.ipynb
```
The analysis relies **only on processed data** from `data/processed` and includes:

- Revenue analysis by country  
- Monthly time-series trend analysis  
- Distribution analysis of order values after winsorization  
- Bootstrap-based comparisons between different groups  
- Exporting visualizations to `reports/figures`  

---

## Weekly Work Breakdown

### Day 1
- Loading raw datasets  
- Understanding schema and column semantics  

### Day 2
- Data cleaning  
- Handling missing values  
- Data type standardization  

### Day 3
- Building the analytical table  
- Performing safe and validated joins  

### Day 4
- Exploratory data analysis  
- Visualization  
- Bootstrap statistical comparisons  

### Day 5
- Integrating all steps into a unified ETL pipeline  
- Adding run metadata logging  
- Writing summaries and documentation  
- Preparing the project for final submission  

---

## Data Quality Notes
- Dataset size is relatively small  
- Some statistical results may not be conclusive  
- Winsorization was applied to reduce the impact of extreme values  
- The analysis reflects only the available time range and does not represent long-term trends  

## How to Run the Project (Quick Start)

```bash
python -m venv .venv
pip install -r requirements.txt
python scripts/run_etl.py
```

After execution, you should see:
- Parquet files under `data/processed`
- `_run_meta.json` with execution details

You can then open `notebooks/eda.ipynb` and run the EDA analysis.

---

## Summary
This project demonstrates a complete, reproducible ETL and EDA workflow, emphasizing data quality, traceability, and analytical readiness. It reflects a production-oriented mindset while remaining suitable for academic evaluation and extension.


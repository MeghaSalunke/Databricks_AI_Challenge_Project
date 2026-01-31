# Databricks_AI_Challenge_Project
Phase 2
# 🏥 Hospital Overloading Monitoring System  
### Databricks AI Challenge Project

## 📌 Project Overview
Hospitals often face overload in specific departments such as Emergency or ICU, even when the overall hospital capacity is not full. This project builds a **hospital-agnostic AI-based monitoring system** that predicts **department-level overload risk** using historical admission, patient flow, and operational data.

The system is designed using **Databricks Lakehouse architecture** and validated using **public Kaggle datasets**. It can later be deployed in real hospitals using local data under proper privacy controls.

---

## 🎯 Problem Statement
To predict **department-level patient load imbalance** in hospitals and classify overload risk as **LOW, MEDIUM, or HIGH**, enabling proactive operational planning.

---

## 🏗 Architecture Used
- Bronze → Silver → Gold (Medallion Architecture)
- Delta Lake tables
- PySpark for transformations
- Machine Learning for risk prediction
- MLflow for experiment tracking
- Dashboard for visualization

---

## 📂 Repository Structure
- `datasets/` – Raw Kaggle datasets used for development
- `silver_layer/` – Data cleaning, validation, schema standardization
- `gold_layer/` – Aggregated metrics and overload risk logic
- `ml_ready/` – ML model training and evaluation
- `mlflow/` – Experiment tracking
- `outputs/` – Dashboard and model output screenshots

---

## 🤖 Machine Learning
- Model: Random Forest Classifier
- Metrics: Accuracy, Precision, Recall, Confusion Matrix
- Input Features:
  - Total admissions
  - Average triage time
  - Registration delay
  - Nurse-patient ratio
  - Specialist availability
  - Forecasted ER load

---

## 📊 Outputs
- Department-level overload risk dashboard
- Confusion matrix and feature importance
- Daily and monthly risk trends

---

## 🔮 Future Scope
- Integration with real hospital systems
- Real-time overload prediction
- Hospital-specific threshold tuning
- Multi-hospital network optimization

---

## ⚠️ Disclaimer
This project uses **public datasets for system design and validation only**. No real patient data is used. In real-world deployment, hospital-owned data would be required under strict governance and privacy controls.

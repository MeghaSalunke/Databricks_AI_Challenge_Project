# ML Flow – Hospital Overload Risk Prediction

## 📌 Overview
This module trains and evaluates a Machine Learning model to predict **department-level overload risk (LOW, MEDIUM, HIGH)** using aggregated hospital operational data from the **Gold layer**.  
The entire training, evaluation, and tracking process is managed using **MLflow on Databricks**.

---

## 📊 Input Data
- Source Table: `default.gold_department_load_risk`
- Data Level: **Gold (Curated & Aggregated)**
- Target Variable: `overload_risk`
- Features Used:
  - total_admissions
  - avg_triage_time_min
  - avg_registration_time_min
  - avg_nurse_patient_ratio
  - avg_specialist_availability
  - forecasted_er_load

---

## 🧠 Model Used
- Algorithm: **RandomForestClassifier**
- Reason:
  - Handles non-linear patterns well
  - Works effectively with mixed operational metrics
  - Provides feature importance for explainability

---

## 🧪 Model Evaluation
The model is evaluated using the following metrics:

- **Accuracy** – Overall correctness of predictions  
- **Precision** – How many predicted overload cases were correct  
- **Recall** – How many actual overload cases were correctly identified  
- **F1-Score** – Balance between precision and recall  
- **Confusion Matrix** – Visual comparison of predicted vs actual risk levels  

These metrics help ensure the model is reliable for operational decision support.

---

## 📈 MLflow Tracking
MLflow is used to track the complete experiment lifecycle:

### Logged Information
- Model parameters (algorithm, hyperparameters)
- Evaluation metrics (accuracy, precision, recall, F1-score)
- Trained model artifact
- Feature importance file
- Experiment runs and versions

### MLflow Features Used
- Experiment tracking
- Model logging
- Model registry (for versioning)
- Artifact storage

---

## 🏗 Workflow Summary
1. Load Gold layer data from Delta table
2. Encode target variable (`overload_risk`)
3. Split data into training and testing sets
4. Train RandomForest model
5. Evaluate model using classification metrics
6. Log metrics, parameters, and model using MLflow
7. Register model for future use

---

## 🚀 Outcome
The trained model provides a **predictive risk classification** that can be:
- Integrated into dashboards
- Used for batch predictions
- Retrained later using real hospital data
- Deployed as a decision-support system in hospitals

---

## ⚠️ Note
This model is intended for **operational planning and decision support**, not for direct clinical diagnosis.



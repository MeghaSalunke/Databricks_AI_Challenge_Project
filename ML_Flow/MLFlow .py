# Databricks notebook source
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

# Load data
df = spark.table("default.gold_department_load_risk").toPandas()

# Encode target
le = LabelEncoder()
df["target"] = le.fit_transform(df["overload_risk"])

X = df[
    [
        "total_admissions",
        "avg_triage_time_min",
        "avg_registration_time_min",
        "avg_nurse_patient_ratio",
        "avg_specialist_availability",
        "forecasted_er_load"
    ]
]

y = df["target"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model = RandomForestClassifier(
    n_estimators=100,
    random_state=42
)

model.fit(X_train, y_train)

preds = model.predict(X_test)

print("Accuracy:", accuracy_score(y_test, preds))
print(classification_report(y_test, preds))


# COMMAND ----------

from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt

# Create confusion matrix
cm = confusion_matrix(y_test, preds)

# Display confusion matrix
disp = ConfusionMatrixDisplay(
    confusion_matrix=cm,
    display_labels=le.classes_  # LOW, MEDIUM, HIGH
)

disp.plot(cmap="Blues")
plt.title("Confusion Matrix – Overload Risk Prediction")
plt.show()


# COMMAND ----------

import pandas as pd

feature_importance = pd.DataFrame({
    "feature": X.columns,
    "importance": model.feature_importances_
}).sort_values(by="importance", ascending=False)

feature_importance


# COMMAND ----------

# DBTITLE 1,MLflow Overload Risk Model
# =========================
# MLflow Overload Risk Model
# =========================

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow.models import infer_signature

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, classification_report

# -------------------------
# 1. Load Gold Data
# -------------------------
df = spark.table("default.gold_department_load_risk").toPandas()

# Encode target
label_encoder = LabelEncoder()
df["target"] = label_encoder.fit_transform(df["overload_risk"])

# Features & target
features = [
    "total_admissions",
    "avg_triage_time_min",
    "avg_registration_time_min",
    "avg_nurse_patient_ratio",
    "avg_specialist_availability",
    "forecasted_er_load"
]

X = df[features]
y = df["target"]

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# -------------------------
# 2. MLflow Experiment
# -------------------------
mlflow.set_experiment("/Shared/Hospital_Overload_Prediction")

with mlflow.start_run(run_name="RandomForest_Overload_Risk"):

    # -------------------------
    # 3. Train Model
    # -------------------------
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=8,
        random_state=42
    )

    model.fit(X_train, y_train)

    # -------------------------
    # 4. Predictions & Metrics
    # -------------------------
    preds = model.predict(X_test)

    accuracy = accuracy_score(y_test, preds)
    f1 = f1_score(y_test, preds, average="weighted")

    print("Accuracy:", accuracy)
    print("F1 Score:", f1)
    print(classification_report(y_test, preds, target_names=label_encoder.classes_))

    # -------------------------
    # 5. Log Parameters
    # -------------------------
    mlflow.log_param("model_type", "RandomForestClassifier")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 8)
    mlflow.log_param("features", features)

    # -------------------------
    # 6. Log Metrics
    # -------------------------
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("f1_score", f1)

    # -------------------------
    # 7. Log Model
    # -------------------------
    signature = infer_signature(X_test, preds)
    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="overload_risk_model",
        registered_model_name="Hospital_Overload_Risk_Model",
        signature=signature
    )

    # -------------------------
    # 8. Feature Importance
    # -------------------------
    feature_importance = pd.DataFrame({
        "feature": features,
        "importance": model.feature_importances_
    }).sort_values(by="importance", ascending=False)

    feature_importance_path = "feature_importance.csv"  # Local path instead of /dbfs/tmp/
    feature_importance.to_csv(feature_importance_path, index=False)

    mlflow.log_artifact(feature_importance_path)

print("MLflow run completed successfully.")

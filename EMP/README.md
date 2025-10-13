## Employee Retention Prediction Model

#### About The Project

This project builds a machine learning model to predict employee retention (whether an employee will leave the company) using various employee-related features.

##### Dataset

The model uses an employee retention dataset (dummy data as real data are classified and come from DFPS data warehouse) with the following features:

- satisfaction_level: Employee satisfaction level (0-1)
- last_evaluation: Last performance evaluation score
- number_project: Number of projects assigned
- average_montly_hours: Average monthly working hours
- time_spend_company: Years spent at the company
- Work_accident: Whether the employee had a work accident (0/1)
- promotion_last_5years: Promotion in last 5 years (0/1)
- salary: Salary level (low/medium/high)
- left: Target variable - whether employee left (1) or stayed (0)

Dataset size: 14,999 employees

##### Data Preprocessing

1. Missing Values: Filled 2 missing values in satisfaction_level with the mean

2. Feature Engineering:

- Dropped empid column (not predictive)
- Applied one-hot encoding to the categorical salary feature
- Created dummy variables for salary levels (low/medium)
- Dropped the original salary column

##### Model Selection

Two models were evaluated using GridSearchCV with 5-fold cross-validation:

1. Random Forest Classifier

- Best accuracy: 91.7%
- Best parameters: criterion='gini', max_depth=3, max_features='log2', n_estimators=130

2. XGBoost Classifier ⭐

- Best accuracy: 99.1%
- Best parameters: learning_rate=0.1, max_depth=20, n_estimators=200
- Selected as the final model due to superior performance

3. Model Performance

- Test Accuracy: 99.2%
- Confusion Matrix Results:
  - True Negatives: 2,291
  - False Positives: 8
  - False Negatives: 16
  - True Positives: 685

##### Deployment

Model is deployed to Azure through Azure Functions. Specifically, I deployed machine learning model as an Azure Function, which is a serverless solution that automatically scales and only charges for actual usage time. This allows HR staff of my department to get real-time retention predictions through a simple API call via Tableau dashboard.

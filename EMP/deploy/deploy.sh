# Login to Azure
az login

# Create resource group
az group create --name employee-churn-rg --location eastus

# Create storage account
az storage account create --name employeechurnstorage --resource-group employee-churn-rg

# Create function app
az functionapp create --resource-group employee-churn-rg \
  --consumption-plan-location eastus \
  --runtime python --runtime-version 3.9 \
  --functions-version 4 \
  --name employee-churn-function \
  --storage-account employeechurnstorage

# Deploy the function
func azure functionapp publish employee-churn-function
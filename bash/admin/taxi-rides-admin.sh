#!/bin/bash

# login "taxi-riders-admin" user
read -sp "For taxi-rides-admin set Azure password: " AZ_PASSWD
az login --username "taxi-rides-admin@asdf169.onmicrosoft.com" --password ${AZ_PASSWD}

# ---------------------------
# --- Setup storage ---------

# create storage account
az storage account create --name "whantanataxirides" \
--resource-group "taxi-rides-rg" \
--location "westeurope" --kind StorageV2  \
--sku Standard_LRS --enable-hierarchical-namespace true

# create container
az storage container create --name "taxi-rides-container" \
--account-name "whantanataxirides" \
--resource-group "taxi-rides-rg"

# list directory
az storage fs directory list --file-system "taxi-rides-container" --path /  \
--account-name "whantanataxirides" --auth-mode login

# list ACLs
az storage fs access show  --file-system "taxi-rides-container" --path /  \
--account-name "whantanataxirides" --auth-mode login

# create "taxi-rides-group"  AD group
az ad group create --display-name "taxi-rides-group" --mail-nickname "TaxiRides"

# set read and execute permissions to "taxi-rides-group"
az storage fs access set-recursive \
--acl "user::rwx,group::r-x,other::---,group:$(az ad group show --group "taxi-rides-group" --query "objectId" --output tsv):r-x" \
--file-system "taxi-rides-container" --path /  \
--account-name "whantanataxirides" --auth-mode login

# -----------------------------------------------------------------------------------
# --- Setup keyvault and taxi-rides-keyvault-secret-reader managed identity ---------

# create key vault for keeping "taxi-rides-data-ingestion" secret and credential
az keyvault create --name "taxi-rides-key-vault" \
--resource-group "taxi-rides-rg" \
--location "westeurope" \
--enable-rbac-authorization true

# set this identity role as Key Vault Reader
az role assignment create --assignee "$(az account show --query "id" --output tsv)" \
--role "Key Vault Administrator" \
--scope "$(az keyvault show --name "taxi-rides-key-vault" --query id --output tsv)"

# create user-assinged managed identity
az identity create --name "taxi-rides-keyvault-secret-reader" \
--resource-group "taxi-rides-rg"

# get service principal ID of the user-assinged managed identity
spID=$(az identity show --name  "taxi-rides-keyvault-secret-reader" \
--resource-group "taxi-rides-rg" \
--query "principalId" --output "tsv")

# set this identity role as Key Vault Reader
az role assignment create --assignee "$(az identity show --name  "taxi-rides-keyvault-secret-reader" --resource-group "taxi-rides-rg" --query "principalId" --output "tsv")" \
--role "Key Vault Secrets User" \
--scope "$(az keyvault show --name "taxi-rides-key-vault" --query id --output tsv)"

# --------------------------------------
# --- Setup container registry ---------

# create container instances
az acr create --name "taxiridescontainerregistry" --resource-group "taxi-rides-rg" --sku Basic

# show login server
az acr show --name "taxiridescontainerregistry" --query loginServer --output table

# ------------------------------------------------------------------------------------
# --- Create taxi-rides-data-ingestion service principal and custom ACI permissions --

# create service principal "taxi-rides-data-ingestion"
SPN_SECRET=$(az ad sp create-for-rbac --name "taxi-rides-data-ingestion" \
--role "Storage Blob Contributor" \
--scopes "$(az storage account show --name "whantanataxirides" --query "id" --output tsv)/blobServices/default/containers/taxi-rides-container" \
--query "password" --output tsv)
  
# set data ingestion secret
az keyvault secret set --name "taxi-rides-data-ingestion-secret" \
--value $SPN_SECRET \
--description "taxi-rides-data-ingestion-secret" \
--vault-name "taxi-rides-key-vault"  

# define custom RBAC role
az role definition create --role-definition /taxi_rides/json/taxi_rides_container_instance_operator.json

# assign custom RBAC role
az role assignment create --assignee "$(az ad sp list --display-name "taxi-rides-data-ingestion" --query "[0].objectId"  --output tsv)" \
--scope "$(az group show --resource-group "taxi-rides-rg" --query "id" --output tsv)" \
--role "Taxi Rides Container Instance Operator"
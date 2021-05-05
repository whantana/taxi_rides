#!/bin/bash

# read "taxi-riders-admin" password for login
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
az storage fs directory list --file-system "taxi-rides-container" --path /  --account-name "whantanataxirides" --auth-mode login

# list ACLs
az storage fs access show  --file-system "taxi-rides-container" --path /  --account-name "whantanataxirides" --auth-mode login

# create "taxi-rides-group"  AD group
az ad group create --display-name "taxi-rides-group" --mail-nickname "TaxiRides"
GROUP_ID=$(az ad group show --group "taxi-rides-group" --query "objectId" --output tsv)

# set read and execute permissions to "taxi-rides-group"
az storage fs access set-recursive \
--acl "user::rwx,group::r-x,other::---,group:${GROUP_ID}:r-x" \
--file-system "taxi-rides-container" --path /  --account-name "whantanataxirides" --auth-mode login

# get id of storage account
stID=$(az storage account show --name "whantanataxirides" --query id --output tsv)

# create service principal "taxi-rides-data-ingestion"
sps=$(az ad sp create-for-rbac --name "taxi-rides-data-ingestion" \
--role "Storage Blob Contributor" \
--scopes "${stID}/blobServices/default/containers/taxi-rides-container" \
--query 'password' --output tsv
)

# ----------------------------
# --- Setup keyvault ---------

# create key vault for keeping "taxi-rides-data-ingestion" secret and credential
az keyvault create --name "taxi-rides-key-vault" \
--resource-group "taxi-rides-rg" \
--location "westeurope" \
--enable-rbac-authorization true

# get keyvault id
kvID=$(az keyvault show --name 'taxi-rides-key-vault' --query id --output tsv)

# taxi-rides-admin user id
tauID=$(az account show --query "id" --output tsv)

# set this identity role as Key Vault Reader
az role assignment create --assignee $tauID --role "Key Vault Administrator" --scope $kvID

# set data ingestion secret
az keyvault secret set --name "taxi-rides-data-ingestion-secret" \
--value $sps \
--description "taxi-rides-data-ingestion-secret" \
--vault-name "taxi-rides-key-vault"

# create user-assinged managed identity
az identity create --name "taxi-rides-keyvault-secret-reader" \
--resource-group "taxi-rides-rg"

# get service principal ID of the user-assinged managed identity
spID=$(az identity show --name  "taxi-rides-keyvault-secret-reader" \
--resource-group "taxi-rides-rg" \
--query "principalId" --output "tsv")

# set this identity role as Key Vault Reader
az role assignment create --assignee $spID --role "Key Vault Secrets User" --scope $kvID

# --------------------------------------
# --- Setup container registry ---------

# create container instances
az acr create --name "taxiridescontainerregistry" --resource-group "taxi-rides-rg" --sku Basic

# show login server
az acr show --name "taxiridescontainerregistry" --query loginServer --output table

# get regisry id
acrID=$(az acr show --name "taxiridescontainerregistry" --query id --output tsv)

# get object id of ingestion
spID=$(az ad sp list --display-name "taxi-rides-data-ingestion" --query '[0].objectId'  --output tsv)

# assign roles to push and pull containers TODO include permissions to custom role
az role assignment create --assignee $spID --scope $acrID --role "acrpull"
az role assignment create --assignee $spID --scope $acrID --role "acrpush"

# TODO Assignment of custom role for taxi-rides-dataingestion






#!/bin/bash

# read "taxi-riders-admin" password for login
read -sp "For taxi-rides-admin set Azure password: " AZ_PASSWD
az login --username "taxi-rides-admin@asdf169.onmicrosoft.com" --password "${AZ_PASSWD}"

# create "taxi-rides-group"  AD group
az ad group create --display-name "taxi-rides-group" --mail-nickname "TaxiRides"
GROUP_ID=$(az ad group show --group "taxi-rides-group" --query "objectId" --output tsv)

# create storage account
az storage account create --name "whantanataxirides" \
--resource-group "taxi-rides-rg" \
--location westeurope --kind StorageV2  \
--sku Standard_LRS --enable-hierarchical-namespace true

# create container
az storage container create --name "taxi-rides-container" \
--account-name "whantanataxirides" \
--resource-group "taxi-rides-rg"

# list directory
az storage fs directory list --file-system "taxi-rides-container" --path /  --account-name "whantanataxirides" --auth-mode login

# list ACLs
az storage fs access show  --file-system "taxi-rides-container" --path /  --account-name "whantanataxirides" --auth-mode login

# set read and execute permissions to "taxi-rides-group"
az storage fs access set-recursive \
--acl "user::rwx,group::r-x,other::---,group:${GROUP_ID}:r-x" \
--file-system "taxi-rides-container" --path /  --account-name "whantanataxirides" --auth-mode login

# create service principal "taxi-rides-data-ingestion"
az ad sp create-for-rbac --name "taxi-rides-data-ingestion" \
--role "Storage Blob Contributor" \
--scopes "/subscriptions/680d3d81-8146-4e4a-8f8b-0a7b6160302d/resourceGroups/taxi-rides-rg/providers/Microsoft.Storage/storageAccounts/whantanataxirides/blobServices/default/containers/taxi-rides-container"
SP_OBJECT_ID=$(az ad sp list --show-mine | jq .[].objectId | sed -e 's/^"//' -e 's/"$//')

# log out
az logout

#!/bin/bash

# login as admin (interactive)
az login

# -----------------------------------------------
# --- Setup Resource Group & Admin user ---------

# create user
read -sp "For taxi-rides-admin set Azure password: " AZ_PASSWD
az ad user create --display-name "taxi-rider-admin" \
--password "${AZ_PASSWD}" \
--user-principal-name "taxi-rides-admin@asdf169.onmicrosoft.com" \
--force-change-password-next-login false

# create resource group
az group create --location westeurope --name "taxi-rides-rg"

# assign "Owner" role to user "taxi-rides-admin" on resource group "taxi-rides-rg"
az role assignment create --assignee "taxi-rides-admin@asdf169.onmicrosoft.com" \
--role "Owner" \
--resource-group "taxi-rides-rg"

az logout
#!/bin/bash
# A simple Azure Storage example script

export AZURE_STORAGE_ACCOUNT=$1
export container_name=$2

end=`date -d "1 year" '+%Y-%m-%dT%H:%M:%SZ'`
echo "Expire time: $end"
sas_token=$(az storage account generate-sas --permissions rwdlacup --account-name $AZURE_STORAGE_ACCOUNT --services bfqt --resource-types sco --expiry $end -otsv)

echo "Creating the container...$container_name"
container_url=https://$AZURE_STORAGE_ACCOUNT.dfs.core.windows.net/$container_name
echo "Container: $container_url"
container_url_sas=$container_url?$sas_token

container_url_sas_clean="${container_url_sas//\"/}"
echo "Container clean path: $container_url_sas_clean"
azcopy make $container_url_sas_clean
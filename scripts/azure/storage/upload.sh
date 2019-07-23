#!/bin/bash
# A simple Azure Storage example script

export AZURE_STORAGE_ACCOUNT=$0
export AZURE_STORAGE_KEY=$1

export container_name=contenedor_datos
export blob_name=nombre_blob
export file_to_upload=MuestraDatos.csv
export destination_file=muestraSubido.csv

echo "Creating the container..."
az storage container create --name $container_name

echo "Uploading the file..."
az storage blob upload --container-name $container_name --file $file_to_upload --name $blob_name

echo "Listing the blobs..."
az storage blob list --container-name $container_name --output table

echo "Downloading the file..."
az storage blob download --container-name $container_name --name $blob_name --file $destination_file --output table

echo "Done"
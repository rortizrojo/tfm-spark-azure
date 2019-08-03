#!/usr/bin/env bash

ficheroInput="muestraSubido.csv"
outputPath="/fichero.csv"


cluster_info=`az hdinsight list --resource-group  grupoRecursosTfm`
cluster_name=`echo $cluster_info | jq -r ".[0].name"`
sshUser=`echo $cluster_info | jq -r ".[0].properties.computeProfile.roles[0].osProfile.linuxOperatingSystemProfile.username"`
pathAccount=`echo $cluster_info | jq -r ".[0].properties.storageProfile.storageaccounts[0].name"`
DATA_LAKE_MAIN_PATH=abfs://contenedoralmacenamiento@${pathAccount}/
sshHostName=${sshUser}@${cluster_name}-ssh.azurehdinsight.net

command="hdfs dfs -cp $DATA_LAKE_MAIN_PATH$ficheroInput $outputPath"
echo "Comando: $command"
echo "SSH HOST: $sshHostName"
sshpass -p 'tfmPassword.2019' ssh -tt $sshHostName -o StrictHostKeyChecking=no "$command"

exit
#!/usr/bin/env bash

cluster_info=`az hdinsight list --resource-group  grupoRecursosTfm`
cluster_name=`echo $cluster_info | jq -r ".[0].name"`
sshUser=`echo $cluster_info | jq -r ".[0].properties.computeProfile.roles[0].osProfile.linuxOperatingSystemProfile.username"`
pathAccount=`echo $cluster_info | jq -r ".[0].properties.storageProfile.storageaccounts[0].name"`
contenedor=`echo $cluster_info | jq -r ".[0].properties.storageProfile.storageaccounts[0].fileSystem"`
DATA_LAKE_MAIN_PATH=abfs://${contenedor}@${pathAccount}/
sshHostName=${sshUser}@${cluster_name}-ssh.azurehdinsight.net
test="hdfs dfs -ls $DATA_LAKE_MAIN_PATH > ficheroSalida"
echo $test
echo "SSH HOST: $sshHostName"
RESULTS=$(sshpass -v -p 'tfmPassword.2019' ssh -tt $sshHostName -o StrictHostKeyChecking=no 'mkdir carpetaTest;$test')
echo "Resultados: $RESULTS"


exit
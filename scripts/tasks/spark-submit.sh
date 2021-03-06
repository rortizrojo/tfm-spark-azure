#!/usr/bin/env bash

grupo_recursos=$1
ficheroInput=input/`cut -d' ' -f1 <<<$2`
spark_submit_args=input/$2
sshPassword=$3


cluster_info=`az hdinsight list --resource-group $grupo_recursos`
cluster_name=`echo $cluster_info | jq -r ".[0].name"`
sshUser=`echo $cluster_info | jq -r ".[0].properties.computeProfile.roles[0].osProfile.linuxOperatingSystemProfile.username"`
pathAccount=`echo $cluster_info | jq -r ".[0].properties.storageProfile.storageaccounts[0].name"`
container=`echo $cluster_info | jq -r ".[0].properties.storageProfile.storageaccounts[0].fileSystem"`
workerNodeSize=`echo $cluster_info | jq -r ".[0].properties.computeProfile.roles[1].hardwareProfile.vmSize"`
workerInstances=`echo $cluster_info | jq -r ".[0].properties.computeProfile.roles[1].targetInstanceCount"`

case "$workerNodeSize" in
   "standard_d12_v2")
        ram="22g"
        cores="4"
   ;;
   "standard_d13_v2")
        ram="45g"
        cores="8"
   ;;
   "standard_d14_v2")
        ram="90g"
        cores="16"
   ;;

esac

DATA_LAKE_MAIN_PATH=abfs://${container}@${pathAccount}/
sshHostName=${sshUser}@${cluster_name}-ssh.azurehdinsight.net


echo "Usuario actual : $USER"

sudo chown -v $USER ~/.ssh/known_hosts
sudo chown -v $USER /home/rortizrojo/.ssh/known_hosts

echo "Ejecutando ssh-keygen rortizrojo"
sudo ssh-keygen -f "/home/rortizrojo/.ssh/known_hosts" -R "${cluster_name}-ssh.azurehdinsight.net"
echo "Ejecutando ssh-keygen jenkins"
ssh-keygen -f "/var/lib/jenkins/.ssh/known_hosts" -R "${cluster_name}-ssh.azurehdinsight.net"


# Copia de ficheros al cluster
echo "Subiendo ficheros al cluster"
sshpass -p $sshPassword scp -o StrictHostKeyChecking=no target/tfmSpark-1.0-SNAPSHOT-jar-with-dependencies.jar $sshHostName:cleaning_lib.jar
sshpass -p $sshPassword scp -rp -o StrictHostKeyChecking=no resources $sshHostName:resources

# Declaración de comandos a ejecutar en cluster
commandCreateInputFolder1="hdfs dfs -mkdir /user/sshuser"
commandCreateInputFolder2="hdfs dfs -mkdir /user/sshuser/input"
commandCopyResources="hdfs dfs -put resources resources"
command="hdfs dfs -cp ${DATA_LAKE_MAIN_PATH}$ficheroInput $ficheroInput"
commandExecuteSparkSubmit="spark-submit --conf spark.yarn.maxAppAttempts=1 --num-executors $workerInstances --executor-memory $ram --executor-cores $cores --master yarn --deploy-mode cluster --class tfm.Main cleaning_lib.jar $spark_submit_args"
# El comando para eliminar la carpeta resources se ejecuta al final del proceso para que en la siguiente ejecución con la meta recursivamente
commandDeleteFolder="rm -rf resources/"

echo "SSH Hostname: $sshHostName"

#Ejecución de comandos
echo "Ejecutando comando: $commandCreateInputFolder1"
echo "Ejecutando comando: $commandCreateInputFolder2"
echo "Ejecutando comando: $commandCopyResources"
echo "Ejecutando comando: $command"
echo "Ejecutando comando: $commandExecuteSparkSubmit"
echo "Ejecutando comando: $commandDeleteFolder"

sshpass -p $sshPassword ssh -tt $sshHostName -o StrictHostKeyChecking=no "$commandCreateInputFolder1;$commandCreateInputFolder2;$commandCopyResources;$command;$commandExecuteSparkSubmit;$commandDeleteFolder"

exit
#!/bin/bash
set -e

FLIGHT_SEARCH_DATE='2020-03-29'
# FLIGHT_SEARCH_DATE= date +'%Y-%m-%d'

JOB_STEPS='[{"Type": "CUSTOM_JAR","Name": "Pyspark_Job","Args": ["spark-submit","--deploy-mode","cluster","--conf","spark.yarn.submit.waitAppCompletion=true","s3://your_bucket/your_folder/python_file.py","'$FLIGHT_SEARCH_DATE'"],"ActionOnFailure": "TERMINATE_CLUSTER","Jar": "command-runner.jar"}]'

JOB_INSTANCES='[{"InstanceCount":2,"InstanceType":"m5.xlarge","InstanceGroupType":"CORE","Name":"Core - 2","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":200}}]}},{"InstanceCount":1,"InstanceType":"m5.xlarge","InstanceGroupType":"MASTER","Name":"Master - 1"}]'

JOB_NAME='EMRJobName'

echo 'Running JOB'

aws emr create-cluster \
--applications Name=Hadoop Name=Hive Name=Spark \
--ec2-attributes '{"KeyName":"xxxxxxxxxxxxx","InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"xxxxxxx","SubnetId":"xxxx-xxxxx","EmrManagedSlaveSecurityGroup":"xx-xxxxxx","EmrManagedMasterSecurityGroup":"xx-xxxxxxxx"}' \
--release-label emr-5.29.0 \
--log-uri 's3://your_bucket_for_log_path/' \
--bootstrap-actions Path="s3://your_bucket/your_folder/s3_files/emr_bootstrap.sh" \
--steps "$JOB_STEPS" \
--instance-groups "$JOB_INSTANCES" \
--auto-terminate \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--service-role EMR_DefaultRole \
--enable-debugging \
--name "$JOB_NAME" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region xx-xxxx-xx \
--configurations https://your_bucket/your_folder/s3_files/configurations.json

echo 'End'

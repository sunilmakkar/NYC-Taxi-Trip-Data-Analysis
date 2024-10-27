# Start the cluster
aws emr create-cluster \
--name "NYC Taxi Analysis Cluster" \
--release-label emr-6.12.0 \
--applications Name=Spark \
--ec2-attributes KeyName=MyEMRKeyPair \
--instance-groups \
    InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
    InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
--use-default-roles \
--log-uri s3://emr-transformations/logs/

# Upload a script to S3 Bucket
aws s3 cp master.py s3://emr-transformations/scripts/master.py

# Add and Run the Step
aws emr add-steps \
--cluster-id j-3VWH2JGY3YOQF \
--steps Type=Spark,Name="NYC Taxi Analysis",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,s3://emr-transformations/scripts/master.py]

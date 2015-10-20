./compileModel.sh

export YARN_CONF_DIR=/etc/hadoop/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
/usr/hdp/current/spark-client/bin/spark-submit --class org.apache.spark.examples.mllib.BinaryClassification --master yarn-cluster  --num-executors 3 --driver-memory 512m  --executor-memory 512m   --executor-cores 1 truckml.jar --algorithm LR --regType L2 --regParam 1.0 /user/root/truck_training --numIterations 200

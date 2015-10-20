scalac -classpath /usr/hdp/current/spark-client/lib/*:/root/truckml/scopt_2.10-3.2.0.jar BinaryClassification.scala 
rm truckml.jar 2>/dev/null


if ! [ -d "./scopt" ]; then
	$JAVA_HOME/bin/jar xf scopt_2.10-3.2.0.jar
fi

$JAVA_HOME/bin/jar cf truckml.jar org scopt

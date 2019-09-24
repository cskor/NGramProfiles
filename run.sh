#!/bin/bash

gradle build
$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
$HADOOP_HOME/bin/hadoop fs -rm -r /cs435/NGramProfile/P1Results
$HADOOP_HOME/bin/hadoop jar build/libs/NGramProfile.jar cs435.hadoop.profileOne.ProfileOneJob /cs435/NGramProfile/largeInputs /cs435/NGramProfile/P1Results
echo Finished Profile One!
$HADOOP_HOME/bin/hadoop fs -copyToLocal /cs435/NGramProfile/P1Results .

$HADOOP_HOME/bin/hadoop fs -rm -r /cs435/NGramProfile/P2Results
$HADOOP_HOME/bin/hadoop jar build/libs/NGramProfile.jar cs435.hadoop.profileTwo.ProfileTwoJob /cs435/NGramProfile/largeInputs /cs435/NGramProfile/P2Results
echo Finished Profile Two!
$HADOOP_HOME/bin/hadoop fs -copyToLocal /cs435/NGramProfile/P2Results .

$HADOOP_HOME/bin/hadoop fs -rm -r /cs435/NGramProfile/P3Results
$HADOOP_HOME/bin/hadoop jar build/libs/NGramProfile.jar cs435.hadoop.profileThree.ProfileThreeJob /cs435/NGramProfile/largeInputs /cs435/NGramProfile/P3Results
echo Finished Profile Three!
$HADOOP_HOME/bin/hadoop fs -copyToLocal /cs435/NGramProfile/P3Results .
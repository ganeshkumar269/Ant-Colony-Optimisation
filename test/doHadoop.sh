#! /bin/sh
#when connection refused occurs -> sudo service ssh restart
javac MR2.java -d units2 -cp $(/home/ganeshkumar269/hadoop/hadoop-3.3.0/bin/hadoop classpath):. &&
jar -cvf units2.jar -C units2/ . &&
~/hadoop/hadoop-3.3.0/bin/hadoop jar units2.jar hadoop.MR2 input_dir2 output_dir2
#~/hadoop/hadoop-3.3.0/bin/hadoop fs -ls output_dir/ &&
~/hadoop/hadoop-3.3.0/bin/hadoop fs -cat output_dir2/part-r-00000 
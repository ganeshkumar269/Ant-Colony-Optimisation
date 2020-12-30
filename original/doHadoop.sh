#! /bin/sh
#when connection refused occurs -> sudo service ssh restart
javac MREA2.java -d units2 -cp $(/home/ganeshkumar269/hadoop/hadoop-3.3.0/bin/hadoop classpath):. &&
jar -cvf units2.jar -C units2/ . &&
~/hadoop/hadoop-3.3.0/bin/hadoop jar units2.jar hadoop.MREA2 input_dir output_dir
# ~/hadoop/hadoop-3.3.0/bin/hadoop fs -ls output_dir/ &&
# ~/hadoop/hadoop-3.3.0/bin/hadoop fs -cat output_dir/part-r-00000 
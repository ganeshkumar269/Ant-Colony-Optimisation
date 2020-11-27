#! /bin/sh
#when connection refused occurs -> sudo service ssh restart
javac ProcessUnits.java -d units -cp $(/home/ganeshkumar269/hadoop/hadoop-3.3.0/bin/hadoop classpath):. &&
jar -cvf units.jar -C units/ . &&
bin/hadoop jar units.jar hadoop.ProcessUnits input_dir output_dir &&
bin/hadoop fs -ls output_dir/ &&
bin/hadoop fs -cat output_dir/part-r-00000 
sudo rm -R /tmp/*   
rm -rf /usr/local/hadoop_store/hdfs/datanode/*
sudo service ssh restart
~/hadoop/hadoop-3.3.0/bin/hdfs namenode -format
~/hadoop/hadoop-3.3.0/sbin/start-dfs.sh
~/hadoop/hadoop-3.3.0/sbin/start-yarn.sh
~/hadoop/hadoop-3.3.0/bin/hdfs dfs -mkdir /user
~/hadoop/hadoop-3.3.0/bin/hdfs dfs -mkdir /user/ganeshkumar269
~/hadoop/hadoop-3.3.0/bin/hadoop fs -mkdir input_dir 
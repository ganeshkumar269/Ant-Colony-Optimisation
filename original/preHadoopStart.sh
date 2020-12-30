sudo rm -R /tmp/*   
rm -rf /usr/local/hadoop_store/hdfs/datanode/*
sudo service ssh restart
hdfs namenode -format
sbin/start-dfs.sh
sbin/start-yarn.sh
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/ganeshkumar269
bin/hadoop fs -mkdir input_dir 
bin/hadoop fs -put sample.txt input_dir 
package hadoop;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MREA2{
    public static class EMapper extends  Mapper<LongWritable ,Text,Text,Text>{
        //intput -> (key, {task,csid,csqos})
        //output -> (task,{csid,csqos})
        public void map(LongWritable key, Text value, 
        Context context) throws IOException,InterruptedException { 
            StringTokenizer s = new StringTokenizer(value.toString()," ");
            String task = s.nextToken();
            StringBuilder QoS = new StringBuilder();
            String csid = s.nextToken();
            QoS = QoS.append(csid + " ");
            while(s.hasMoreTokens())
                QoS = QoS.append(s.nextToken() + " ");
            context.write(new Text(task),value);
        }
    }
    //output -> (1,{task,csid,csqos})
    public static class EReducerR extends  Reducer<Text ,Text,Text,Text>{

        public void reduce(Text key, Iterable <Text> values, 
        Context context) throws IOException,InterruptedException {    
            ArrayList<ArrayList<Double>> csList = new ArrayList<ArrayList<Double>>();
            ArrayList<Double> cs;
            ArrayList<String> csIds = new ArrayList<String>();
            for(Text value : values){
                System.out.println(key.toString() + " value: " + value.toString());
                StringTokenizer st = new StringTokenizer(value.toString()," ");
                String task = st.nextToken();
                String csid = st.nextToken();
                csIds.add(csid);
                cs = new ArrayList<Double>();
                int cnt = 0;
                for(int i = 0; i < 5; i++){
                    cs.add(Double.parseDouble(st.nextToken()));
                    cnt++;
                }
                System.out.println("cnt: " + cnt);
                csList.add(cs);
            }
            //Skyline
            ArrayList<ArrayList<Double>> domList = new ArrayList<ArrayList<Double>>();
            for(int i = 0;i < csList.size();i++){
                boolean canBeAdded = true;
                for(int j = 0; j < csList.size();j++){
                    int e = 0;
                    int g = 0;
                    int l = 0;
                    if(i == j) continue;
                    for(int k = 0; k < 5; k++){
                        if(csList.get(i).get(k) < csList.get(j).get(k))
                            l += 1;
                        if(csList.get(i).get(k) == csList.get(j).get(k))
                            e += 1;
                        if(csList.get(i).get(k) > csList.get(j).get(k))
                            g += 1;
                    }

                            
                    if(g==0 && e !=5){
                        canBeAdded = false;
                        break;
                    }
                }
                if(canBeAdded == true){
                    String out = new String();
                    out = key.toString() + " " + csIds.get(i) + " ";
                    for(int k = 0 ;k < 5; k++){
                        out = out + String.valueOf(csList.get(i).get(k)) + " ";
                    }
                    context.write(new Text(String.valueOf(1)),new Text(out));
                }
            }
        }
    }
    public static void main(String... args) throws IOException,InterruptedException,ClassNotFoundException{
       Configuration config = new Configuration();
       Job job = Job.getInstance(config,"MREA2");
       FileSystem fs = FileSystem.get(config);
       if (fs.exists(new Path(args[1]))) {
           fs.delete(new Path(String.valueOf(args[1])), true);
       }    
       job.setJarByClass(MREA2.class);
       job.setMapperClass(EMapper.class);
       job.setReducerClass(EReducerR.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
       FileInputFormat.addInputPath(job,new Path(args[0]));
       FileOutputFormat.setOutputPath(job,new Path(args[1]));
       job.waitForCompletion(true);
    }
}
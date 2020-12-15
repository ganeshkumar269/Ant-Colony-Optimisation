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

public class MREA{
    public static class EMapper extends  Mapper<LongWritable ,Text,IntWritable,Text>{
        public void map(LongWritable key, Text value, 
        Context context) throws IOException,InterruptedException { 
            StringTokenizer st = new StringTokenizer(value," ");
            if(!s.nextToken().equals("solution"))
            {
                String task = s.nextToken();
                String QoS = new String();
                while(s.hasMoreTokens())
                    QoS = QoS + s.nextToken() + " ";
                context.write(new IntWritable(Integer.parseInt(task)),new Text(QoS));
            }
        }
    }
    public static class ECombiner extends  Reducer<IntWritable ,Text,IntWritable,Text>{
        public void reduce(Text key, Iterator <Text> values, 
        Context context) throws IOException,InterruptedException {    
            ArrayList<ArrayList<Double>> csList = new ArrayList<ArrayList<Double>>();
            while(values.hasNext()){
                Text value = values.next();
                StringTokenizer st = new StringTokenizer(value," ");
                while(st.hasMoreTokens()){
                    cs = new ArrayList<Double>();
                    cs.add(Double.parseDouble(st.nextToken()));
                }
            }
            //Skyline
            ArrayList<ArrayList<Double>> domList = new ArrayList<ArrayList<Double>>();
            for(int i = 0;i < csList.size();i++){
                boolean canBeAdded = true;
                for(int j = 0; j < csList.size();j++){
                    if(i!=j){
                        int cnt = 0;
                        for(int k = 0; k < 5; k++){
                            if(csList.get(i).get(k) < csList.get(j).get(k)){
                                cnt += 1
                            }
                        }
                        if(cnt == 5)
                            canBeAdded = false;
                    }
                    if(canBeAdded == false)
                        break;
                }
                if(canBeAdded == true){
                    String out = new String();
                    out = out + key.toString() + " ";
                    for(int k = 0 ;k < 5; k){
                        out = out + String.valueOf(csList.get(i).get(k)) + " ";
                    }
                    context.write(new IntWritable(1),new Text(out));
                }
            }
        }
    }
    public static class EReducer extends  Reducer<IntWritable ,Text,IntWritable,Text>{
        public void reduce(Text key, Iterator <Text> values, 
        Context context) throws IOException,InterruptedException { 
            ArrayList<ArrayList<Double>> csList = new  ArrayList<ArrayList<Double>>();
            for(int i = 0;i < 100;i++)
                csList.add(new ArrayList<Double>());
            for(Text i : values){
                String t = i.toString();
                ArrayList<Double> cs = new ArrayList<Double>();
                StringTokenizer st = new StringTokenizer(t, " ");
                int task = Integer.parseInt(st.nextToken());
                for(int k =0; k < 5;k++)
                    cs.add(Double.parseDouble(st.nextToken()));
                csList.get(task).add(cs);
            }
            
            //new Solutions
            // int numOfSols = 1000;
            ArrayList<ArrayList<Double>> sols = new ArrayList<Integer>(); 
            Random rand = new Random();
            int numOfCS = domList.size();
            for(int i = 0; i < numOfSols; i++){
                sols.add(rand.nextInt(numOfCS));
            }
            
            //Generate and Initialise Probablities 
            // ArrayList<Double> prob = new ArrayList<Double>();
            // for(int i = 0; i < numOfCS; i++)
            //     prob.add(1.0/numOfCS);
            
            //Generate Parent Pop

            //Update Prob
            //Guided Mutation
            //Repair Op.
        }
    }
    public static void main(String... args){
        
    }
}
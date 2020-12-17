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
        //intput -> (key, {task,csid,csqos})
        //output -> (task,{csid,csqos})
        public void map(LongWritable key, Text value, 
        Context context) throws IOException,InterruptedException { 
            StringTokenizer s = new StringTokenizer(value," ");
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
    //output -> (1,{task,csid,csqos})
    public static class ECombiner extends  Reducer<IntWritable ,Text,IntWritable,Text>{
        public void reduce(Text key, Iterator <Text> values, 
        Context context) throws IOException,InterruptedException {    
            ArrayList<ArrayList<Double>> csList = new ArrayList<ArrayList<Double>>();
            ArrayList<Double> cs;
            ArrayList<Integer> csIds = new ArrayList<Integer>();
            while(values.hasNext()){
                Text value = values.next();
                StringTokenizer st = new StringTokenizer(value," ");
                int csid = Integer.parseInt(st.nextToken());
                csIds.add(csid);
                cs = new ArrayList<Double>();
                while(st.hasMoreTokens()){
                    cs.add(Double.parseDouble(st.nextToken()));
                }
                csList.add(cs);
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
                    out = key.toString() + " " + String.valueOf(csIds.get(i)) + " ";
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
            HashMap<Integer,ArrayList<ArrayList<Double>> > totalList = new  HashMap<Integer,ArrayList<Double>>(); 
            HashMap<Integer,ArrayList<Integer>> totalids = new  HashMap<Integer,ArrayList<Double>>(); 
            for(int i = 0;i < 100;i++)
                csList.add(new ArrayList<Double>());
            for(Text i : values){
                String t = i.toString();
                ArrayList<Double> cs = new ArrayList<Double>();
                StringTokenizer st = new StringTokenizer(t, " ");
                int task = Integer.parseInt(st.nextToken());
                int csid = Integer.parseInt(st.nextToken());
                
                for(int k =0; k < 5;k++)
                    cs.add(Double.parseDouble(st.nextToken()));
                if(totalList.containsKey(task)){
                    totalList.get(task).add(cs);
                    totalids.get(task).add(csid);
                }
                else{
                    totalList.put(task,new ArrayList<ArrayList<Double>>());
                    totalids.put(task,new ArrayList<Integer>());
                    totalids.get(task).add(csid);
                    totalList.get(task).add(cs);
                }
            }
            
            //new Solutions
            // int numOfSols = 1000;
            ArrayList<ArrayList<Integer>> sols = new ArrayList<ArrayList<Integer>>(); 
            Random rand = new Random();
            int numOfSols = 1000;
            int taskNum = totalList.size();
            for(int i = 0; i < numOfSols; i++)
            {
                ArrayList<Integer> temp = new ArrayList<Integer>();
                for(int j = 0; j < taskNum; j++){
                    temp.add( rand.nextInt( totalids.get(j).size() ) );
                }
                sols.add( temp );
            }
            
            //Generate and Initialise Probablities 
            ArrayList<ArrayList<Double>> prob = new ArrayList<ArrayList<Double>>();
            for(int i =0;i < taskNum; i++){
                ArrayList<Double> temp = new ArrayList<Double>();
                for(int j =0 ;j < totalList.get(i)).size(); j++)
                    temp.add(1.0/totalList.get(i).size());
                prob.add(temp);
            }

            // ArrayList<Double> prob = new ArrayList<Double>();
            // for(int i = 0; i < numOfCS; i++)
            //     prob.add(1.0/numOfCS);
            
            //Generate Parent Pop
            ArrayList<ArrayList<Integer>> parentPop = new ArrayList<ArrayList<Integer>>(); 
            ArrayList<Double>> fitness = new ArrayList<Double>();
            
            for(ArrayList<Integer> i : sols){
                double val = 0;
                double tempVal = 0;
                for(int j = 0; j < taskNum;j++)
                    tempVal += totalList.get(j).get(i.get(j)).get(0);
                val += 0.1417*tempVal;
                tempVal = Double.MAX_VALUE;
                for(int j = 0; j < taskNum;j++)
                    tempVal = min(tempVal,totalList.get(j).get(i.get(j)).get(1));
                val += tempVal;
                tempVal = 1;
                for(int j = 0; j < taskNum;j++)
                    tempVal *= totalList.get(j).get(i.get(j)).get(2);
                val += tempVal;
                tempVal = 1;
                for(int j = 0; j < taskNum;j++)
                    tempVal *= totalList.get(j).get(i.get(j)).get(3);
                val += tempVal;
                tempVal = 0;
                for(int j = 0; j < taskNum;j++)
                    tempVal += totalList.get(j).get(i.get(j)).get(4);
                val += tempVal;
                fitness.add(val);
            }
            ArrayList<Double> temp_fit = new ArrayList<Double>(fitness);
            Collections.sort(temp_fit);
            double th = temp_fit.get(3*fitness.size()/4);
            for(ArrayList<Integer> i : sols){
                if()
            }

            //Update Prob
            //Guided Mutation
            //Repair Op.
        }
    }
    public static void main(String... args){
        
    }
}
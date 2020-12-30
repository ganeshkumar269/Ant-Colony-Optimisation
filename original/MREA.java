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

public class MREA{
    public static class EMapper extends  Mapper<LongWritable ,Text,IntWritable,Text>{
        //intput -> (key, {task,csid,csqos})
        //output -> (task,{csid,csqos})
        public void map(LongWritable key, Text value, 
        Context context) throws IOException,InterruptedException { 
            StringTokenizer s = new StringTokenizer(value.toString()," ");
            String initVal = s.nextToken();
            if(!initVal.equals("solution"))
            {
                String task = initVal;
                String QoS = new String();
                String csid = s.nextToken();
                QoS = csid + " ";
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
            ArrayList<String> csIds = new ArrayList<String>();
            while(values.hasNext()){
                Text value = values.next();
                StringTokenizer st = new StringTokenizer(value.toString()," ");
                String csid = st.nextToken();
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
                    context.write(new IntWritable(1),new Text(out));
                }
            }
        }
    }
    public static class EReducer extends  Reducer<IntWritable ,Text,IntWritable,Text>{
        public void reduce(Text key, Iterator <Text> values, 
        Context context) throws IOException,InterruptedException { 
            ArrayList<ArrayList<Double>> csList = new  ArrayList<ArrayList<Double>>();
            HashMap<Integer,ArrayList<ArrayList<Double>> > totalList = new  HashMap<Integer,ArrayList<ArrayList<Double>>>(); 
            HashMap<Integer,ArrayList<String>> totalids = new  HashMap<Integer,ArrayList<String>>(); 
            for(int i = 0;i < 100;i++)
                csList.add(new ArrayList<Double>());
            while(values.hasNext()){
                String t = values.next().toString();
                ArrayList<Double> cs = new ArrayList<Double>();
                StringTokenizer st = new StringTokenizer(t, " ");
                int task = Integer.parseInt(st.nextToken());
                String csid = st.nextToken();
                
                for(int k =0; k < 5;k++)
                    cs.add(Double.parseDouble(st.nextToken()));
                if(totalList.containsKey(task)){
                    totalList.get(task).add(cs);
                    totalids.get(task).add(csid);
                }
                else{
                    totalList.put(task,new ArrayList<ArrayList<Double>>());
                    totalids.put(task,new ArrayList<String>());
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
                for(int j =0 ;j < totalList.get(i).size(); j++)
                    temp.add(1.0/totalList.get(i).size());
                prob.add(temp);
            }

            // ArrayList<Double> prob = new ArrayList<Double>();
            // for(int i = 0; i < numOfCS; i++)
            //     prob.add(1.0/numOfCS);
            
            //Generate Parent Pop
            ArrayList<ArrayList<Integer>> parentPop = new ArrayList<ArrayList<Integer>>(); 
            ArrayList<Double> fitness = new ArrayList<Double>();
            
            for(ArrayList<Integer> i : sols){
                double val = 0;
                double tempVal = 0;
                for(int j = 0; j < taskNum;j++)
                    tempVal += totalList.get(j).get(i.get(j)).get(0);
                val += 0.1417*tempVal;
                tempVal = Double.MAX_VALUE;
                for(int j = 0; j < taskNum;j++)
                    tempVal = Math.min(tempVal,totalList.get(j).get(i.get(j)).get(1).doubleValue());
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
            double bestFitness = temp_fit.get(temp_fit.size()-1);
            ArrayList<Integer> bestSol = new ArrayList<Integer>(); 
            for(int i = 0;i < sols.size(); i++){
                if(fitness.get(i) >= th)
                    parentPop.add(sols.get(i));
                if(fitness.get(i) == bestFitness)
                    bestSol = sols.get(i);
            }
            
            //Update Prob
            ArrayList<ArrayList<Integer>> cnts = new ArrayList<ArrayList<Integer>>();  
            for(int i =0 ;i < taskNum; i++){
                ArrayList<Integer> cnt = new ArrayList<Integer>();
                for(int j = 0; j < totalList.get(i).size();j++);
                    cnt.add(0);
                cnts.add(cnt);
            } 
            
            for(ArrayList<Integer> i : parentPop){
                for(int j = 0; j < i.size(); j++){
                    cnts.get(j).set( i.get(j) , cnts.get(j).get(i.get(j)) + 1);
                }
            }
            double lambda = 0;
            for(int i = 0;i < totalList.size(); i++){
                for(int j =0; j < totalList.get(i).size(); j++){
                    prob.get(i).set(j , (1-lambda)*prob.get(i).get(j) + ( lambda*4*cnts.get(i).get(j) / sols.size() ) );
                }
            }
            
            //Guided Mutation
            ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>(); 
            double beta = 0;
            double th_prob = 0;
            for(int i = 0;i < taskNum; i++){
                ArrayList<Integer> temp = new ArrayList<Integer>();
                for(int j = 0; j < totalList.get(i).size(); j++){
                    double r = Math.random();
                    if( r < beta){
                        if(prob.get(i).get(j) < th_prob){
                            String out = new String();
                            out = totalids.get(i).get(j);
                            for(int k =0 ;k < 5;k++){
                                out = out + totalList.get(i).get(j).get(k) + " ";
                            }
                            context.write(new IntWritable(i),new Text(out));
                        }
                        else{
                            if(bestSol.get(i) == j){
                                String out = new String();
                                out = totalids.get(i).get(j);
                                for(int k =0 ;k < 5;k++){
                                    out = out + totalList.get(i).get(j).get(k) + " ";
                                }
                                int testKey = Integer.parseInt(key.toString());
                                context.write(new IntWritable(testKey),new Text(out));
                            }
                        }
                    }
                }
                result.add(temp);
            }
            
            
            //Repair Op.


        }
    }
    public static void main(String... args) throws IOException,InterruptedException,ClassNotFoundException{
       Configuration config = new Configuration();
       Job job = Job.getInstance(config,"MREA");
       FileSystem fs = FileSystem.get(config);
       job.setJarByClass(MREA.class);
       job.setMapperClass(EMapper.class);
       job.setCombinerClass(ECombiner.class);
       job.setReducerClass(EReducer.class);
       job.setOutputKeyClass(IntWritable.class);
       job.setOutputValueClass(Text.class);
       FileInputFormat.addInputPath(job,new Path(args[0]));
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(String.valueOf(args[1])), true);
        }    
       FileOutputFormat.setOutputPath(job,new Path(args[1]));
       job.waitForCompletion(true);
    }
}
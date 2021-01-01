package hadoop; 

import java.util.*; 
import java.io.*;


import java.io.IOException; 
import java.io.IOException; 

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class MR2{
   //Mapper class 
    public static class customComp implements Comparator<ArrayList<Double>>{
        public int compare(ArrayList<Double> a, ArrayList<Double> b){
            return a.get(a.size()-1).compareTo(b.get(b.size()-1));
        }
    }
    public static class EMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        public void map(LongWritable key, Text val,Context context)
            throws IOException, InterruptedException{
                ArrayList<ArrayList<Double>> sol = new ArrayList<ArrayList<Double>>(); 
                StringTokenizer st = new StringTokenizer(val.toString()," ");
                int part = key.get();
                for(int i = 0;i < taskNum; i++){
                    ArrayList<Double> temp = new ArrayList<Double>();
                    String csid = st.nextToken();
                    for(int j =0 ;j < itemsPerRow;j++)
                        temp.add(Double.parseDouble(st.nextToken()));
                    sol.add(temp);
                }
                context.write(new IntWritable(part),new Text(val.toString() + " " + (new Fitness(sol)).calculate()));
            }
    }
    public static class EReducer extends Reducer<Text,Tex,Text,IntWritable>{
        public void reduce(Text key, Iterable<Text> val,Context context)
            throws IOException, InterruptedException{
                //select parent solutions
                ArrayList<ArrayList<Double>> subPop = new ArrayList<ArrayList<Double>>();
                ArrayList<ArrayList<Double>> parentSols = new ArrayList<ArrayList<Double>>();
                ArrayList<Double> sol;
                for(Text i : val){
                   sol = new ArrayList<Double>();
                   StringTokenizer st = new StringTokenizer(i," ");
                   while(st.hasMoreTokens())
                    sol.add(Double.parseDouble(st.nextToken()));
                    subPop.add(sol);
                }
                Collections.sort(subPop,new customComp());
                for(int i = 0; i < subPop.size()/4; i++)
                    parentSols.add(subPop.get(i));
                
                // Probabilities and guided mutation
                ArrayList<ArrayList<Double>> prob = new ArrayList<ArrayList<Double>>();
                for(int i =0;i < taskNum; i++){
                    ArrayList<Double> temp = new ArrayList<Double>();
                    for(int j =0 ;j < totalList.get(i).size(); j++)
                        temp.add(1.0/totalList.get(i).size());
                    prob.add(temp);
                }
                
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
                double lambda = 0.34;
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
            }   
    }
    public static class EPartitioner extends Partitioner<IntWritable,Text>{
        public int getPartition(IntWritable key, Text val, int num){
            return key.get() % num;
        }
    }

    public static void main(String... args) throws IOException, InterruptedException, ClassNotFoundException{
        int taskNum = 10;
        //Get Data
        Dataset dataset = new Dataset(taskNum);
        PrintWriter pw = new PrintWriter("inputfile.txt");
        //Skyline
        ArrayList<ArrayList<ArrayList<Double>>> totalList = new ArrayList<ArrayList<ArrayList<Double>>>(); 
        for(int task = 0 ; task < taskNum; task++){
            ArrayList<ArrayList<Double>> domList = new ArrayList<ArrayList<Double>>();
            for(int i = 0;i < dataset.getRowsPerTask();i++){
                boolean canBeAdded = true;
                for(int j = 0; j < dataset.getRowsPerTask();j++){
                    int e = 0;
                    int g = 0;
                    int l = 0;
                    if(i == j) continue;
                    for(int k = 0; k < 5; k++){
                        if(dataset.getItem(task,i,k) < dataset.getItem(task,j,k))
                            l += 1;
                        if(dataset.getItem(task,i,k) == dataset.getItem(task,j,k))
                            e += 1;
                        if(dataset.getItem(task,i,k) > dataset.getItem(task,j,k))
                            g += 1;
                    }
                            
                    if(g==0 && e !=5){
                        canBeAdded = false;
                        break;
                    }
                }
                if(canBeAdded == true){
                    // pw.write(task + " ");
                    // for(int val : dataset.getRow(task,i))
                    //     pw.write(val + " ");
                    // pw.write("\n")
                    domList.add(dataset.getRow(task,i));
                }
            }
            totalList.add(domList);
        }

        //generate solutions
        int numOfSols = 100;
        for(int i = 0;i < numOfSols; i++){
            for(int j =0 ;j < taskNum; j++){
                int index = Math.random()*totalList.get(j).size();
                for(int val : totalList.get(j).get(index))
                    pw.write(val + " ");
            }
            pw.write("\n");
        }
        pw.flush();
        //Map-Reduce Phase
        Configuration config = new Configuration();
        Job job = Job.getInstance(config,"test MR1");
        FileSystem fs = FileSystem.get(config);
        fs.copyFromLocalFile(true,true,new Path("./inputfile.txt"),new Path(args[0]));
        if (fs.exists(new Path(args[1]))) 
        fs.delete(new Path(String.valueOf(args[1])), true);
        job.setJarByClass(MR2.class);
        job.setMapperClass(EMapper.class); 
        job.setReducerClass(EReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormat(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        //Generate Initial Probs
        ArrayList<ArrayList<Double>> prob = new ArrayList<ArrayList<Double>>();
        for(int i =0;i < taskNum; i++){
            ArrayList<Double> temp = new ArrayList<Double>();
            for(int j =0 ;j < dataset.getRowsPerTask(); j++)
                temp.add(1.0/dataset.getRowsPerTask());
            prob.add(temp);
        }
        //Get Reducer output
        ArrayList<ArrayList<Integer>> reducerOutput = new  ArrayList<ArrayList<Integer>>();
        //keep track of the max fitness of the reducer outputs
        double maxFitnessSoFar = 0;
        //new totalList wrt reducer Output
        ArrayList<ArrayList<Integer>> newTotalList = new ArrayList<ArrayList<Integer>>();
        
        //initialise cnts
        ArrayList<ArrayList<Integer>> cnts = new ArrayList<ArrayList<Integer>>();  
        for(int i =0 ;i < taskNum; i++){
            ArrayList<Integer> cnt = new ArrayList<Integer>();
            for(int j = 0; j < dataset.getRowsPerTask();j++);
                cnt.add(0);
            cnts.add(cnt);
        } 
        //update cnts
        for(ArrayList<Integer> i : reducerOutput){
            for(int j = 0; j < i.size(); j++){
                cnts.get(j).set( i.get(j) , cnts.get(j).get(i.get(j)) + 1);
            }
        }
        //update probabilities using Alg-2
        double lambda = 0.34;
        for(int i = 0;i < taskNum; i++){
            for(int j =0; j < dataset.getRowsPerTask()  ; j++){
                prob.get(i).set(j , (1-lambda)*prob.get(i).get(j) + ( lambda*4*cnts.get(i).get(j) / sols.size() ) );
            }
        }

        ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>(); 
        
        //Generate New Population using Alg-3
        double beta = 0.035;
        double th_prob = 0.55;
        for(int i = 0;i < taskNum; i++){
            ArrayList<Integer> temp = new ArrayList<Integer>();
            for(int j = 0; j < totalList.get(i).size(); j++){
                double r = Math.random();
                if( r < beta){
                    if(prob.get(i).get(j) < th_prob){
                        newTotalList.get(i).add(j);
                        // String out = new String();
                        // out = totalids.get(i).get(j);
                        // for(int k =0 ;k < 5;k++){
                        //     out = out + totalList.get(i).get(j).get(k) + " ";
                        // }
                        // context.write(new IntWritable(i),new Text(out));
                    }
                    // else{
                    //     if(bestSol.get(i) == j){
                    //         String out = new String();
                    //         out = totalids.get(i).get(j);
                    //         for(int k =0 ;k < 5;k++){
                    //             out = out + totalList.get(i).get(j).get(k) + " ";
                    //         }
                    //         int testKey = Integer.parseInt(key.toString());
                    //         context.write(new IntWritable(testKey),new Text(out));
                    //     }
                    // }
                }
            }
            result.add(temp);
        }

        //generate new solutions
        
        pw = new PrintWriter("inputfile.txt");
        int part;
        for(int i =0 ;i < numOfSols; i++){
            StringBuilder out = new StringBuilder();
            out = out.append(String.valueOf((int)i%part));
            for(int j = 0; j < taskNum; j++){
                int csid = newTotalList.get(j).get(Math.random()*newTotalList.get(j).size());
                out.append(String.valueOf(csid) + " ");
                for(Double k : totalList.get(j).get(csid))
                    out = out.append(String.valueOf(k) + " ");
            }
        }
        fs.copyFromLocalFile(true,true,new Path("./inputfile.txt"),new Path(args[0]));
    }
}
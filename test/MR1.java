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
    public static class EMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        public void map(LongWritable key, Text val,Context context)
            throws IOException, InterruptedException{
                ArrayList<ArrayList<Double>> sol = new ArrayList<ArrayList<Double>>(); 
                StringTokenizer st = new StringTokenizer(val.toString()," ");
                int part = key.get();
                for(int i = 0;i < taskNum; i++){
                    ArrayList<Double> temp = new ArrayList<Double>();
                    for(int j =0 ;j < itemsPerRow;j++)
                        temp.add(Double.parseDouble(st.nextToken()));
                    sol.add(temp);
                }
                context.write(new IntWritable(part),new Text(val.toString() + " " + (new Fitness(sol)).calculate()));
            }
    }
    public static class EReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> val,Context context)
            throws IOException, InterruptedException{
                int cnt = 0;
                for(IntWritable i : val)
                    cnt++;
                context.write(key,new IntWritable(cnt));
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
        //Map-Reduce Phase
        Configuration config = new Configuration();
        Job job = Job.getInstance(config,"test MR1");
        FileSystem fs = FileSystem.get(config);
        fs.copyFromLocalFile(false,true,new Path("./inputfile.txt"),new Path(args[0]));
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
    }
}
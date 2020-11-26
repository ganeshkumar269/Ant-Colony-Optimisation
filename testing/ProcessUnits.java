package hadoop; 

import java.util.*; 

import java.io.IOException; 
import java.io.IOException; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import Ant2;




public class ProcessUnits {
   //Mapper class 


   public static class E_EMapper extends  
   Mapper<LongWritable ,/*Input key Type */ 
   Text,                /*Input value Type*/ 
   Text,                /*Output key Type*/ 
   Text>        /*Output value Type*/ 
   {
      //Map function 
      public void map(LongWritable key, Text value, 
      Context context) throws IOException,InterruptedException { 
         
         String line = value.toString(); 
         // String lasttoken = null; 
         StringTokenizer s = new StringTokenizer(line," ");
         int antid = Integer.parseInt(s.nextToken());
         int taskNum = Integer.parseInt(s.nextToken());
         int w = Integer.parseInt(s.nextToken());
         int h = Integer.parseInt(s.nextToken());
         ArrayList<ArrayList<ArrayList<Double>>> cost;
         ArrayList<ArrayList<ArrayList<Double>>> pher;
         ArrayList<ArrayList<Double>> datasetArray;
         cost = new ArrayList<ArrayList<ArrayList<Double>>>(); 
         pher = new ArrayList<ArrayList<ArrayList<Double>>>(); 
         datasetArray = new ArrayList<ArrayList<Double>>();
         ArrayList<Double> wtList;
         ArrayList<String> aggList;
         wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
         aggList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));
         int rowsPerTask = w;
         int itemsPerRow = 5;
         ArrayList<ArrayList<Double>> tempMatrix = new ArrayList<ArrayList<Double>>();
         ArrayList<Double> tempRow = new ArrayList<Double>();
         for(int i = 0; i < w; i++)
            tempRow.add(Double.parseDouble(s.nextToken()));
         tempMatrix.add(tempRow);
         cost.add(tempMatrix);
         for(int i = 0;i < taskNum-1;i++){
            tempMatrix.clear();
            for(int j = 0; j < w; j++){
               tempRow.clear();
               for(int k = 0; k < w; k++){
                  tempRow.add(Double.parseDouble(s.nextToken()));
               }
               tempMatrix.add(tempRow);
            }
            cost.add(tempMatrix);
         }

         tempMatrix.clear();
         tempRow.clear();

         for(int i = 0; i < w; i++)
            tempRow.add(Double.parseDouble(s.nextToken()));
         tempMatrix.add(tempRow);
         pher.add(tempMatrix);
         for(int i = 0;i < taskNum-1;i++){
            tempMatrix.clear();
            for(int j = 0; j < w; j++){
               tempRow.clear();
               for(int k = 0; k < w; k++){
                  tempRow.add(Double.parseDouble(s.nextToken()));
               }
               tempMatrix.add(tempRow);
            }
            pher.add(tempMatrix);
         }

         tempRow.clear();
         for(int i = 0;i < 2500;i++){
            tempRow.clear();
            for(int j = 0;j < 5; j++){
               tempRow.add(Double.parseDouble(s.nextToken()));
            }
            datasetArray.add(tempRow);
         }

         Ant2 ant = new Ant2(taskNum,cost,pher,wtList,aggList,datasetArray);
         ant.move();
         String result = new String();
         result = String.valueOf(ant.getFitness());
         // for(int j = 0; j < taskNum; j++)
         //    result = result + String.valueOf(ant.trail.get(j)) + " ";
         context.write(new Text(""+antid),new Text(result));
         // String year = s.nextToken(); 
         // while(s.hasMoreTokens()) {
         //    output.collect(new Text(s.nextToken()), new IntWritable(1));          
         // }
         // int antid = Integer.parseInt(line);
         // System.out.println(ants.size());
         // ants.get(antid).move();
         // output.collect(new IntWritable(antid),new DoubleWritable(ants.get(antid).getFitness()));
      }

   }
   
   //Reducer class 
   public static class E_EReduce extends Reducer< Text, Text, Text,Text > {
   
      //Reduce function 
      public void reduce( Text key, Iterator <Text> values, 
      Context context) throws IOException,InterruptedException { 
         // int antid = key.get();
         // double fitness = 0;
         // while (values.hasNext()) { 
         //    fitness = values.next().get();
         // }
         context.write(key,values.next());
         // output.collect(key, new DoubleWritable(fitness)); 
         // output.collect(key, new IntWritable(cnt)); 
         // output.collect(new Text(key+"_"), new IntWritable(tempVal)); 
      } 
   }
   public static volatile ArrayList<Ant2> ants = new ArrayList<Ant2>();
   public static ACO2 aco;
   //Main function 

   public static void main(String args[])throws Exception { 
      // int taskNum = 20;
      // int numOfAnts = 10;
      // aco = new ACO2(taskNum);
      // for(int i = 0;i < numOfAnts; i++)
      //    ants.add(new Ant2(taskNum,aco.cost,aco.pher,aco.wtList,aco.aggList,aco.dataset));
      // System.out.println("Ants Created");

      // JobConf conf = new JobConf(ProcessUnits.class); 
      // Configuration config = new Configuration();

      // conf.setJobName("max_eletricityunits"); 
      // conf.setOutputKeyClass(Text.class);
      // conf.setOutputValueClass(IntWritable.class); 
      // conf.setMapperClass(E_EMapper.class); 
      // conf.setCombinerClass(E_EReduce.class); 
      // conf.setReducerClass(E_EReduce.class); 
      // conf.setInputFormat(TextInputFormat.class); 
      // conf.setOutputFormat(TextOutputFormat.class);
      // // conf.set("mapreduce.map.speculative","false");
      // // conf.set("mapreduce.reduce.speculative","false");
      // FileSystem fs = FileSystem.get(config);
		// if (fs.exists(new Path(args[1]))) {
		// 	fs.delete(new Path(String.valueOf(args[1])), true);
		// }
      // FileInputFormat.setInputPaths(conf, new Path(args[0])); 
      // FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
      
      // long startTime = System.currentTimeMillis();
      
      
      // JobClient.runJob(conf); 


      // long endTime = System.currentTimeMillis();
      // System.out.println("Time Taken: " + (endTime-startTime) + "ms");
         
      Configuration config = new Configuration();
      Job job = Job.getInstance(config, "testing_stuff");  
      FileSystem fs = FileSystem.get(config);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(String.valueOf(args[1])), true);
		}    
      job.setJarByClass(ProcessUnits.class);
      job.setMapperClass(E_EMapper.class); 
      job.setCombinerClass(E_EReduce.class); 
      job.setReducerClass(E_EReduce.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      // job.setNumMapTasks(1);
      // job.setNumReduceTasks(1);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.waitForCompletion(true);


      // System.out.println("Another One");
      // FileInputFormat.setInputPaths(conf, new Path(args[1])); 
      // FileOutputFormat.setOutputPath(conf, new Path(args[1]+"1")); 
      // JobClient.runJob(conf); 
   } 
}
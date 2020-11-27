package hadoop; 

import java.util.*; 
import java.io.*;

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
      ArrayList<ArrayList<ArrayList<Double>>> cost;
      ArrayList<ArrayList<ArrayList<Double>>> pher;
      ArrayList<ArrayList<Double>> datasetArray;
      ArrayList<Double> wtList;
      ArrayList<String> aggList;
      
      int taskNum;
      int rowsPerTask;
      int itemsPerRow = 5;

      protected void setup(Mapper.Context context)
        throws IOException, InterruptedException {
         cost = new ArrayList<ArrayList<ArrayList<Double>>>(); 
         pher = new ArrayList<ArrayList<ArrayList<Double>>>(); 
         datasetArray = new ArrayList<ArrayList<Double>>();
         wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
         aggList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));
         StringTokenizer s;
         taskNum = Integer.parseInt(context.getConfiguration().get("taskNum"));
         int w = 2500/taskNum;
         rowsPerTask = w;

         String inputCost = context.getConfiguration().get("cost");
         String inputPher = context.getConfiguration().get("pher");
         String inputDataset = context.getConfiguration().get("cost");
         
         s = new StringTokenizer(inputCost," ");
         
         ArrayList<ArrayList<Double>> tempMatrix = new ArrayList<ArrayList<Double>>();
         ArrayList<Double> tempRow = new ArrayList<Double>();
         for(int i = 0; i < w; i++)
            tempRow.add(Double.parseDouble(s.nextToken()));
         tempMatrix.add(tempRow);
         cost.add(tempMatrix);
         for(int i = 0;i < taskNum-1;i++){
            tempMatrix = new ArrayList<ArrayList<Double>>();
            for(int j = 0; j < w; j++){
               tempRow = new ArrayList<Double>();
               for(int k = 0; k < w; k++){
                  tempRow.add(Double.parseDouble(s.nextToken()));
               }
               tempMatrix.add(tempRow);
            }
            cost.add(tempMatrix);
         }

         s = new StringTokenizer(inputPher," ");

         tempRow = new ArrayList<Double>();
         tempMatrix = new ArrayList<ArrayList<Double>>();

         for(int i = 0; i < w; i++)
            tempRow.add(Double.parseDouble(s.nextToken()));
         tempMatrix.add(tempRow);
         pher.add(tempMatrix);
         for(int i = 0;i < taskNum-1;i++){
            tempMatrix = new ArrayList<ArrayList<Double>>();
            for(int j = 0; j < w; j++){
               tempRow = new ArrayList<Double>();
               for(int k = 0; k < w; k++){
                  tempRow.add(Double.parseDouble(s.nextToken()));
               }
               tempMatrix.add(tempRow);
            }
            pher.add(tempMatrix);
         }

         s = new StringTokenizer(inputDataset," ");
         for(int i = 0;i < 2500;i++){
            tempRow = new ArrayList<Double>();
            for(int j = 0;j < 5; j++){
               tempRow.add(Double.parseDouble(s.nextToken()));
            }
            datasetArray.add(tempRow);
         }


      }


      //Map function 
      public void map(LongWritable key, Text value, 
      Context context) throws IOException,InterruptedException { 

         String antid = value.toString();
         
         

         Ant2 ant = new Ant2(taskNum,cost,pher,wtList,aggList,datasetArray);
         ant.move();
         StringBuilder result = new StringBuilder();
            result.append(String.valueOf(ant.getFitness()) + " ") ;
         for(int j = 0; j < taskNum; j++)
            result.append(String.valueOf(ant.trail.get(j)) + " ");
         context.write(new Text(antid),new Text(result.toString()));
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
   public static Dataset dataset;
   public static ArrayList<ArrayList<ArrayList<Double>>> cost;
   public static ArrayList<ArrayList<ArrayList<Double>>> pher;
   public static ArrayList<Double> wtList;
   public static ArrayList<String> aggList;
   public static int taskNum = 50;
   public static int itemsPerRow ;
   public static int rowsPerTask ;

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
      
      taskNum = 20;
      dataset = new Dataset(taskNum);
      itemsPerRow = 5;
      rowsPerTask = 2500/taskNum;
      Double bestValSofar = 0.0;
      wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
      aggList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));
      cost = new ArrayList<ArrayList<ArrayList<Double>>>();
      pher = new ArrayList<ArrayList<ArrayList<Double>>>();
        
      StringBuilder qwsData = new StringBuilder();
      BufferedReader br = new BufferedReader(new FileReader("qws2_csv_normalised_4.csv"));
      String nextline = br.readLine();   
      StringTokenizer tokenizer;

      while((nextline = br.readLine()) != null){
         tokenizer = new StringTokenizer(nextline,",");
         ArrayList<String> vals = new ArrayList<String>();
         while(tokenizer.hasMoreTokens())
               vals.add(tokenizer.nextToken());
         
         qwsData = qwsData.append(vals.get(6) + " ");   
         qwsData = qwsData.append(vals.get(2) + " ");   
         qwsData = qwsData.append(vals.get(1) + " ");   
         qwsData = qwsData.append(vals.get(4) + " ");   
         qwsData = qwsData.append(vals.get(0) + " ");

        }
         generateMatrices();

         StringBuilder pw1 = new StringBuilder();
         writeMatrix(pw1,cost);

         StringBuilder pw2 = new StringBuilder();
         writeMatrix(pw2,pher);



      Configuration config = new Configuration();
      config.set("cost",pw1.toString());
      config.set("pher",pw2.toString());
      config.set("dataset",qwsData.toString());
      config.set("taskNum",String.valueOf(taskNum));
      int itr = 5;
      for(int i = 0;i < itr; i++){

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
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         job.waitForCompletion(true);

         InputStream is = fs.open(new Path("output_dir/part-r-00000"));
         try {
            Properties props = new Properties();
            props.load(new InputStreamReader(is, "UTF8"));
            for (Map.Entry prop : props.entrySet()) {
              String name = (String)prop.getKey();
              String value = (String)prop.getValue();
            //   System.out.println("Value: " + value);
               double fitnessVal = updateTrail(value);
               bestValSofar = Math.max(bestValSofar,fitnessVal);
            }
          } 
          catch(Exception e){
            e.printStackTrace();
          }
          finally {
              is.close();
          }
          pw2 = new StringBuilder();
          writeMatrix(pw2,pher);
          config.set("pher",pw2.toString());
          System.out.println("BestValSoFar: " + bestValSofar);
          
      }


      // System.out.println("Another One");
      // FileInputFormat.setInputPaths(conf, new Path(args[1])); 
      // FileOutputFormat.setOutputPath(conf, new Path(args[1]+"1")); 
      // JobClient.runJob(conf); 

      // Job job2 = Job.getInstance(config, "testing_stuff2");  
		// if (fs.exists(new Path(args[1]+"1"))) {
		// 	fs.delete(new Path(String.valueOf(args[1]+"1")), true);
		// }    
      // job2.setJarByClass(ProcessUnits.class);
      // job2.setMapperClass(E_EMapper2.class); 
      // job2.setCombinerClass(E_EReduce2.class); 
      // job2.setReducerClass(E_EReduce2.class);
      // job2.setOutputKeyClass(Text.class);
      // job2.setOutputValueClass(Text.class);
      // job.setNumMapTasks(1);
      // job.setNumReduceTasks(1);
      // FileInputFormat.addInputPath(job2, new Path(args[1]));
      // FileOutputFormat.setOutputPath(job2, new Path(args[1]+"1"));
      // job.waitForCompletion(true);   
   } 
   private static void writeMatrix(StringBuilder pw,ArrayList<ArrayList<ArrayList<Double>>> arr){
      for(int i = 0;i < arr.size();i++){
          for(int j = 0;j < arr.get(i).size(); j++){
              for(int k = 0; k < arr.get(i).get(j).size(); k++){
                  pw.append(String.valueOf( arr.get(i).get(j).get(k) ) + " ");
              }
          }
      }
  }
  private static void generateMatrices(){
   cost.add(generateCostMatrix(-1,1,dataset.getRowsPerTask()));
   for(int i = 0; i < taskNum-1; i++)
       cost.add(generateCostMatrix(i,dataset.getRowsPerTask(),dataset.getRowsPerTask()));
   
   pher.add(generatePherMatrix(-1,1,dataset.getRowsPerTask()));
   for(int i = 0; i < taskNum-1; i++)
       pher.add(generatePherMatrix(i,dataset.getRowsPerTask(),dataset.getRowsPerTask()));
   }
   private static ArrayList<ArrayList<Double>> generateCostMatrix(int task,int r,int c){
      ArrayList<ArrayList<Double>> matrix = new ArrayList<ArrayList<Double>>();
      ArrayList<Double> tempRow;
      for(int i =0; i < r; i++){
          tempRow = new ArrayList<Double>();
          for(int j = 0; j < c;j++){
              tempRow.add(getDistance(task,i,j));
          }
          matrix.add(tempRow);
      }
      return matrix;
  }
  private static ArrayList<ArrayList<Double>> generatePherMatrix(int task,int r,int c){
   ArrayList<ArrayList<Double>> matrix = new ArrayList<ArrayList<Double>>();
   ArrayList<Double> tempRow;
   for(int i =0; i < r; i++){
       tempRow = new ArrayList<Double>();
       for(int j = 0; j < c;j++){
           tempRow.add(Math.random());
       } 
       matrix.add(tempRow);
   }
   return matrix;
   }
   private static double getDistance(int task,int i, int j){
      double dist = 0;
      if(task == -1)
          for(int attr = 0; attr < dataset.getItemsPerRow(); attr++)
              dist += wtList.get(attr)*dataset.getItem(0,j,attr);
      else
          for(int attr = 0; attr < dataset.getItemsPerRow(); attr++){
              switch(aggList.get(attr)){
                  case "sum": dist += wtList.get(attr)*(dataset.getItem(task,i,attr)+dataset.getItem(task+1,j,attr));break;
                  case "mul": dist += wtList.get(attr)*(dataset.getItem(task,i,attr)*dataset.getItem(task+1,j,attr));break;
                  case "min": dist += wtList.get(attr)*Math.min(dataset.getItem(task,i,attr),dataset.getItem(task+1,j,attr));break;
                  case "max": dist += wtList.get(attr)*Math.max(dataset.getItem(task,i,attr),dataset.getItem(task+1,j,attr));break;
              }
          }
      return dist;
  }
   public static double updateTrail(String value){
         // System.out.println("UpdateTrailCalled");
         StringTokenizer s= new StringTokenizer(value," ");
         double contrib = Double.parseDouble(s.nextToken());
         ArrayList<Integer> trail = new ArrayList<Integer>();
         for(int i = 0;i < taskNum;i++)
            trail.add(Integer.parseInt(s.nextToken()));
         pher.get(0).get(0).set(trail.get(0),pher.get(0).get(0).get(trail.get(0))+contrib);
            for(int i = 1; i < taskNum; i++){
                  pher.get(i).get(trail.get(i-1))
                     .set(trail.get(i),
                     pher.get(i).get(trail.get(i-1)).get(trail.get(i))+contrib);
            }
         return contrib;
      }

}
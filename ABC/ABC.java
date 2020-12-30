package hadoop; 

import java.util.*; 
import java.io.*;

import java.io.IOException; 
import java.io.IOException; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
    Map: (key,value)
        

*/
public class ABC {
    public static class E_EMapper extends  
    Mapper<LongWritable, Text, Text, Text>        
    {
      
        public void map(LongWritable key, Text value, 
        Context context) throws IOException,InterruptedException { 
            context.write(new Text("Val"),new Text(String.valueOf(val)));
        }
    }
    public static class E_EReduce extends Reducer< Text, Text, Text,Text > {
        public void reduce( Text key, Iterator <Text> values, 
        Context context) throws IOException,InterruptedException { 
            
        }
    }

    static int swarmSize;
    static int taskNum;
    static int rowsPerTask;
    static ArrayList<ArrayList<Integer>> food;
    static ArrayList<Integer> trail; 
    static ArrayList<Double> obj; 
    static ArrayList<Double> fitness; 
    static ArrayList<Double> prob; 
    static double bestSofar;
    public static void main(String... args) throws IOException, InterruptedException,ClassNotFoundException{
        swarmSize = 100;
        taskNum = 20;
        rowsPerTask = 2500/taskNum;
        food = new ArrayList<ArrayList<Integer>>();
        trail = new ArrayList<Integer>();
        obj = new ArrayList<Double>();
        fitness = new ArrayList<Double>();
        prob = new ArrayList<Double>();

        ArrayList<Integer> temparr = new ArrayList<Integer>();
        for(int i = 0;i < 10; i++)
            temparr.add(i);
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(String.valueOf(args[1])), true);
        }    
         
        // FSDataOutputStream outs = fs.create(new Path("thisiscreated.txt"),true);
        // outs.write("Thi is is created".getBytes());
        // outs.hflush();
        // outs.close();
        
        // FileOutputStream fos = new FileOutputStream("object.dat");
        // ObjectOutputStream oos = new ObjectOutputStream(fos);
        // oos.writeObject(temparr);
        // oos.close();  
        Job job = Job.getInstance(config, "testing_stuff");  
        job.setJarByClass(ABC.class);
        job.setMapperClass(E_EMapper.class); 
        job.setCombinerClass(E_EReduce.class); 
        job.setReducerClass(E_EReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        System.out.println("MR-Over");
        InputStream is = fs.open(new Path("thisiscreated.txt"));
        System.out.println("Char Read: " + (char)is.read());
        System.out.println("Char Read: " + (char)is.read());
        System.out.println("Char Read: " + (char)is.read());
        //Initialisation Phase
        //EmpBee Phase
        //OnLookBee Phase
        //ScoutPhase
    }

}
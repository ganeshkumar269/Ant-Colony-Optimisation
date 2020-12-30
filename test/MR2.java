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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import Ant2;




public class MR2{
   //Mapper class 
    public static class EMapper extends Mapper<Text ,Text,Text,IntWritable>{
        public void map(Text key, Text val,Context context)
            throws IOException, InterruptedException{
                StringTokenizer st = new StringTokenizer(val.toString()," ");
                while(st.hasMoreTokens())
                    context.write(new Text(st.nextToken()),new IntWritable(1));
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

    public static void main(String... args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration config = new Configuration();
        Job job = Job.getInstance(config,"test MR1");
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(new Path(args[1]))) 
        fs.delete(new Path(String.valueOf(args[1])), true);
        job.setJarByClass(MR2.class);
        job.setMapperClass(EMapper.class); 
        job.setReducerClass(EReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
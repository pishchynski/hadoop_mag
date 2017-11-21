package edu.gatech.cse6242;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Task1 extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Task1(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Task1");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable target = new IntWritable();
        private IntWritable weight = new IntWritable();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String tokens[] = line.split("\t");
            target.set(Integer.valueOf(tokens[1]));
            weight.set(Integer.valueOf(tokens[2]));
            
            context.write(target, weight);
        }
        
    }
    
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        protected void reduce(IntWritable target, Iterable<IntWritable> weights, Context context) throws IOException, InterruptedException {
            int sum = 0;
            
            for (IntWritable weight: weights) {
                sum += weight.get();
            }
            
            context.write(target, new IntWritable(sum));
        }
        
    }
}

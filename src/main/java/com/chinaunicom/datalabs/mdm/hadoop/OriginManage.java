package com.chinaunicom.datalabs.mdm.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * dealing with originnal data
 * Created by zhangxr103 on 2014/10/30.
 */
public class OriginManage {


    public static class MapWork
            extends Mapper<Object, Text, PairKey, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            LongWritable from=new LongWritable(Long.parseLong(itr.nextToken()));
            LongWritable to=new LongWritable(Long.parseLong(itr.nextToken()));
            LongWritable time=new LongWritable(Long.parseLong(itr.nextToken()));
            IntWritable duration=new IntWritable(Integer.parseInt(itr.nextToken()));

            IntWritable send_location=new IntWritable(Integer.parseInt(itr.nextToken()));

            IntWritable recieve_location=new IntWritable(Integer.parseInt(itr.nextToken()));

            PairKey<LongWritable, LongWritable> form_to=new PairKey<LongWritable, LongWritable>(from,to);
            PairKey<LongWritable, IntWritable> send_time_loc=new PairKey<LongWritable, IntWritable>(time,send_location);
            PairKey<PairKey<LongWritable, LongWritable>, PairKey<LongWritable, IntWritable>> send_final_key=new PairKey<PairKey<LongWritable, LongWritable>, PairKey<LongWritable, IntWritable>>(form_to,send_time_loc);
            context.write(send_final_key,duration);

            PairKey<LongWritable, LongWritable> to_from=new PairKey<LongWritable, LongWritable>(to,from);
            PairKey<LongWritable, IntWritable> receive_time_lob=new PairKey<LongWritable, IntWritable>(time,recieve_location);
            PairKey<PairKey<LongWritable, LongWritable>, PairKey<LongWritable, IntWritable>> receive_final_key=new PairKey<PairKey<LongWritable, LongWritable>, PairKey<LongWritable, IntWritable>>(to_from,receive_time_lob);
            context.write(receive_final_key,duration);

        }
    }

    public static class ReduceWork
            extends Reducer<PairKey,IntWritable,Text,Text> {
        private Text result = new Text();

        public void reduce(PairKey key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int count=0;

            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }

            result.set(sum+"\t"+count);
            context.write(new Text(key.toString()), result);
        }
    }


    public static class PartitionWork extends Partitioner<LongWritable,Text>{

        @Override
        public int getPartition(LongWritable key, Text value, int numPartitions) {

            return 0;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Origin Manage <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "origin manage");
        job.setJarByClass(OriginManage.class);
        job.setMapperClass(MapWork.class);
        job.setCombinerClass(ReduceWork.class);
        job.setReducerClass(ReduceWork.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(PairKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
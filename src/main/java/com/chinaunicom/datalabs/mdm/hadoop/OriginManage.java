package com.chinaunicom.datalabs.mdm.hadoop;

import com.chinaunicom.datalabs.mdm.util.PairWritable;
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
            extends Mapper<Object, Text, PairWritable, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            LongWritable from=new LongWritable(Long.parseLong(itr.nextToken()));
            LongWritable to=new LongWritable(Long.parseLong(itr.nextToken()));
            LongWritable time=new LongWritable(Long.parseLong(itr.nextToken()));
            IntWritable duration=new IntWritable(Integer.parseInt(itr.nextToken()));

            IntWritable send_location=new IntWritable(Integer.parseInt(itr.nextToken()));

            IntWritable recieve_location=new IntWritable(Integer.parseInt(itr.nextToken()));

            PairWritable<LongWritable, LongWritable> form_to=new PairWritable<LongWritable, LongWritable>();
            form_to.setFirstKey(from);
            form_to.setSecondKey(to);


            PairWritable<LongWritable, IntWritable> send_time_loc=new PairWritable<LongWritable, IntWritable>();
            send_time_loc.setFirstKey(time);
            send_time_loc.setSecondKey(send_location);

            PairWritable  send_final_key=new PairWritable();
            send_final_key.setFirstKey(form_to);
            send_final_key.setSecondKey(send_time_loc);

            context.write(send_final_key,duration);

            PairWritable<LongWritable, LongWritable> to_from=new PairWritable<LongWritable, LongWritable>();
            to_from.setFirstKey(to);
            to_from.setSecondKey(from);

            PairWritable<LongWritable, IntWritable> receive_time_loc=new PairWritable<LongWritable, IntWritable>();
            receive_time_loc.setFirstKey(time);
            receive_time_loc.setSecondKey(recieve_location);

            PairWritable  receive_final_key=new PairWritable ();
            receive_final_key.setFirstKey(to_from);
            receive_final_key.setSecondKey(receive_time_loc);
            context.write(receive_final_key,duration);

        }
    }

    public static class ReduceWork
            extends Reducer<PairWritable,IntWritable,Text,Text> {
        private Text result = new Text();

        public void reduce(PairWritable key, Iterable<IntWritable> values,
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
//        job.setCombinerClass(ReduceWork.class);
        job.setReducerClass(ReduceWork.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

package com.chinaunicom.datalabs.mdm.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
 * 生成 网络数据
 * Created by xinrong on 2015/3/30.
 */
public class SRC2NetDetail {
    public static class MapWork
            extends Mapper<Object, Text, Text, IntWritable> {

        public static Text key_str = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            LongWritable from = new LongWritable(Long.parseLong(itr.nextToken()));
            LongWritable to = new LongWritable(Long.parseLong(itr.nextToken()));
            LongWritable time = new LongWritable(Long.parseLong(itr.nextToken()));
            IntWritable duration = new IntWritable(Integer.parseInt(itr.nextToken()));
            IntWritable send_location = new IntWritable(Integer.parseInt(itr.nextToken()));
            IntWritable recieve_location = new IntWritable(Integer.parseInt(itr.nextToken()));

            key_str.set(from.get() + "\t" + to.get() + "\t" + time.get() + "\t" + send_location.get() + "\t" + 1);
            context.write(key_str, duration);
        }
    }

    public static class ReduceWork
            extends Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;

            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }

            result.set(sum + "\t" + count);
            context.write(key, result);
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop -jar xx.jar SRC2NetDetail <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "dealing user's net data");
        job.setJarByClass(SRC2UserInfo.class);
        job.setMapperClass(MapWork.class);
//        job.setCombinerClass(ReduceWork.class);
        job.setReducerClass(ReduceWork.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

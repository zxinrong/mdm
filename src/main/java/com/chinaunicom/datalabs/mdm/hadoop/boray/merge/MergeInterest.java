package com.chinaunicom.datalabs.mdm.hadoop.boray.merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 合并行 
 * Created by zhangxr103 on 2014/10/30.
 */
public class MergeInterest {

    public static class MapWork
            extends Mapper<Object, Text, LongWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String ss[]=value.toString().trim().split("\\t");
            LongWritable key_word=new LongWritable(Long.parseLong(ss[0]));
            context.write(key_word,new Text(ss[2]));
        }
    }


    public static class ReduceWork
            extends Reducer<LongWritable,Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String word = "";
            String exclude="其他";
            boolean first=true;
            for(Text val:values){
                String value=val.toString();
                if (value.equals(exclude)) continue;
                if(first){
                    word=value;
                    first=false;
                }else if(!word.contains(value)){//去重
                    word+=","+val.toString();
                }
            }
            context.write(key,new Text(word));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop -jar xx.jar SRC2UserInfo <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "merge work");
        job.setJarByClass(MergeInterest.class);
        job.setMapperClass(MapWork.class);
        job.setCombinerClass(ReduceWork.class);
        job.setReducerClass(ReduceWork.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

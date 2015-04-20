package com.chinaunicom.datalabs.mdm.hadoop.boray;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.spark.sql.catalyst.expressions.In;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * 合并行 
 * Created by zhangxr103 on 2014/10/30.
 */
public class PrepareKMeans {
    public static HashMap<Long,Integer> location= Maps.newHashMap();

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

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
                    .getConfiguration());
            File file=new File(cacheFiles[0].toString());
            BufferedReader reader=new BufferedReader(new FileReader(file));
            String temp;
            int count=0;
            while ((temp=reader.readLine())!=null){
                String []ss=temp.split((new byte[]{1}).toString());
                location.put(Long.parseLong(ss[0]),count++);
            }
        }

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {



            String word = "";
            String exclude="其他";
            boolean firest=true;
            for(Text val:values){
                String value=val.toString();
                if (value.equals(exclude)) continue;
                if(firest){
                    word=value;
                    firest=false;
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
            System.err.println("Usage: hadoop -jar xx.jar com.chinaunicom.datalabs.mdm.hadoop.boray.PrepareKMeans <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "prapare vector for kmeans");
        job.setJarByClass(PrepareKMeans.class);
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

        DistributedCache.createSymlink(job.getConfiguration());//
        try {
            DistributedCache.addCacheFile(new URI("/user/hive/warehouse/bj_cu_data.db/item_tbl/000000_0"), job.getConfiguration());
        } catch (URISyntaxException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

/**
 * 合并行
 * Created by zhangxr103 on 2014/10/30.
 */
public class PrepareKMeans {
    public static HashMap<String, Integer> dimMap = Maps.newHashMap();

    public static class MapWork
            extends Mapper<Object, Text, LongWritable, VectorWritable> {
        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            if (dimMap.isEmpty()) {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
                        .getConfiguration());
                File file = new File(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String temp;
                int count = 0;
                while ((temp = reader.readLine()) != null) {
                    String[] ss = temp.split(new String(new byte[]{1}));
                    dimMap.put(ss[0].trim(), count++);
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String ss[] = value.toString().trim().split("\\t");
            LongWritable key_word = new LongWritable(Long.parseLong(ss[1].trim()));
            Vector v = new DenseVector(dimMap.size());
            v.set(dimMap.get(ss[2]), Double.parseDouble(ss[5]));
            VectorWritable vw = new VectorWritable();
            vw.set(v);
            context.write(key_word, vw);
        }
    }

    public static class CombineWork
            extends Reducer<LongWritable, VectorWritable, LongWritable, VectorWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (dimMap.isEmpty()) {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
                        .getConfiguration());
                File file = new File(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String temp;
                int count = 0;
                while ((temp = reader.readLine()) != null) {
                    String[] ss = temp.split(new String(new byte[]{1}));
                    dimMap.put(ss[0].trim(), count++);
                }
            }
        }

        public void reduce(LongWritable key, Iterable<VectorWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Vector v = new DenseVector(dimMap.size());
            for (VectorWritable e : values) {
                Vector tmp = e.get();
                for (int i = 0; i < tmp.size(); i++) {
                    if (tmp.get(i) > 10) {
                        v.set(i, tmp.get(i));
                    }
                }
            }
            VectorWritable result = new VectorWritable();
            result.set(v);

            context.write(key, result);
        }
    }

    public static class ReduceWork
            extends Reducer<LongWritable, VectorWritable, LongWritable, VectorWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (dimMap.isEmpty()) {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
                        .getConfiguration());
                File file = new File(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String temp;
                int count = 0;
                while ((temp = reader.readLine()) != null) {
                    String[] ss = temp.split(new String(new byte[]{1}));
                    dimMap.put(ss[0].trim(), count++);
                }
            }
        }

        public void reduce(LongWritable key, Iterable<VectorWritable> values,
                           Context context
        ) throws IOException, InterruptedException {


            Vector v = new DenseVector(dimMap.size());
            for (VectorWritable e : values) {
                Vector tmp = e.get();
                for (int i = 0; i < tmp.size(); i++) {
                    if (tmp.get(i) > 0) {
                        v.set(i, tmp.get(i));
                    }
                }
            }

//            String word="";
//            boolean first=true;
//            for(int i=0;i<v.size();i++){
//                double value=v.get(i);
//                if(first){
//                    word+=value;
//                    first=false;
//                }else{
//                    word+="\t"+value;
//                }
//            }

            context.write(key, new VectorWritable(v));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop -jar xx.jar com.chinaunicom.datalabs.mdm.hadoop.boray.PrepareKMeans /user/hive/warehouse/bj_cu_data.db/user_interest_tbl/ <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "prapare vector for kmeans");
        job.setJarByClass(PrepareKMeans.class);
        job.setMapperClass(MapWork.class);
        job.setCombinerClass(CombineWork.class);
        job.setReducerClass(ReduceWork.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

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

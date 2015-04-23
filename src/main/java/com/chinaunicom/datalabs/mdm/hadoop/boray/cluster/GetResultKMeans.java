package com.chinaunicom.datalabs.mdm.hadoop.boray.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * Created by zhangxr103 on 2015/4/22.
 */
public class GetResultKMeans {

    public static class MapWork
            extends Mapper<IntWritable,WeightedVectorWritable, WeightedVectorWritable,IntWritable> {

        public void map(IntWritable key, WeightedVectorWritable value, Context context) throws IOException, InterruptedException {
            context.write(value,key);
        }
    }

    public static class PartitionWork
            extends Partitioner <WeightedVectorWritable,IntWritable>{
        @Override
        public int getPartition(WeightedVectorWritable k, IntWritable v, int i) {
            return v.get();
        }
    }

    public static class ReduceWork
            extends Reducer<WeightedVectorWritable, IntWritable, VectorWritable, IntWritable> {

        public void reduce(WeightedVectorWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(new VectorWritable(key.getVector()),values.iterator().next());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop -jar xx.jar com.chinaunicom.datalabs.mdm.hadoop.boray.cluster.GetResultKMeans <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "get result vector for kmeans");
        job.setJarByClass(GetResultKMeans.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(MapWork.class);
        job.setPartitionerClass(PartitionWork.class);
        job.setCombinerClass(ReduceWork.class);
        job.setReducerClass(ReduceWork.class);

        job.setMapOutputKeyClass(WeightedVectorWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(VectorWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

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

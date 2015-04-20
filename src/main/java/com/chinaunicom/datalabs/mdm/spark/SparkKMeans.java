package com.chinaunicom.datalabs.mdm.spark;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.HadoopRDD;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 * Created by xinrong on 2014/10/30.
 */
public class SparkKMeans {
    public static void kmeans(){
        SparkConf sconf=new SparkConf().setAppName("kmeans");

        JavaSparkContext jsc=new JavaSparkContext(sconf);

        JavaRDD<String> rdd= jsc.textFile("");
        rdd.count();
    }

    public static void main(String...args){
        if (args.length < 1) {
            System.err.println("Usage: JavaKMeans <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaKMeans");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile(args[0], 1);

        JavaRDD<Vector> datas=lines.flatMap(new FlatMapFunction<String, Vector>() {
            @Override
            public Iterable<Vector> call(String s) throws Exception {
                return null;
            }
        });

        List<Vector> points = Lists.newArrayList(
                Vectors.dense(1.0, 2.0, 6.0),
                Vectors.dense(1.0, 3.0, 0.0),
                Vectors.dense(1.0, 4.0, 6.0)
        );

        Vector expectedCenter = Vectors.dense(1.0, 3.0, 4.0);

        JavaRDD<Vector> data = sc.parallelize(points, 2);
        KMeansModel model = KMeans.train(data.rdd(), 1, 1, 1, KMeans.K_MEANS_PARALLEL());
        assertEquals(1, model.clusterCenters().length);
        assertEquals(expectedCenter, model.clusterCenters()[0]);

        model = KMeans.train(data.rdd(), 1, 1, 1, KMeans.RANDOM());
    }
}

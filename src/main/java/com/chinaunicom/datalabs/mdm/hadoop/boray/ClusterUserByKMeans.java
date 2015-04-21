package com.chinaunicom.datalabs.mdm.hadoop.boray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.utils.clustering.ClusterDumper;

import java.io.IOException;

/**
 *
 * Created by xinrong on 2015/4/20.
 */
public class ClusterUserByKMeans {
     public static class NewDistanceMeasure  extends SquaredEuclideanDistanceMeasure{
        public NewDistanceMeasure() {
        }

        public double distance(Vector v1, Vector v2) {
            Vector v11=new DenseVector(v1.size()-1);
            Vector v22=new DenseVector(v2.size()-1);
            for(int i=1;i<v1.size();i++){
                v11.set(i-1,v1.get(i));
            }
            for(int i=1;i<v1.size();i++){
                v22.set(i-1,v2.get(i));
            }
            return Math.sqrt(super.distance(v11, v22));
        }

        public double distance(double centroidLengthSquare, Vector v1, Vector v2) {
            Vector v11=new DenseVector(v1.size()-1);
            Vector v22=new DenseVector(v2.size()-1);
            for(int i=1;i<v1.size();i++){
                v11.set(i-1,v1.get(i));
            }
            for(int i=1;i<v1.size();i++){
                v22.set(i-1,v2.get(i));
            }
            return Math.sqrt(super.distance(centroidLengthSquare, v11, v22));
        }
}



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser parser=new GenericOptionsParser(conf, args);
        String[] otherArgs = parser.getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: hadoop jar mdm.jar com.chinaunicom.datalabs.mdm.hadoop.boray.ClusterUserByKMeans <in> <out>");
            System.exit(2);
        }

        String inPath = args[0];
        String seqFile = inPath + "/seqfile";
        String seeds = inPath + "/seeds";
        String outPath = inPath + "/result/";
        String clusteredPoints = outPath + "/clusteredPoints";

//        InputDriver.runJob(new Path(inPath), new Path(seqFile), "org.apache.mahout.math.RandomAccessSparseVector");

        int k = 10;
        Path seqFilePath = new Path(seqFile);
        Path clustersSeeds = new Path(seeds);
        DistanceMeasure measure = new NewDistanceMeasure();
        clustersSeeds = RandomSeedGenerator.buildRandom(conf, seqFilePath, clustersSeeds, k, measure);
        KMeansDriver.run(conf, seqFilePath, clustersSeeds, new Path(outPath), measure, 0.01, 10, true,0.01, false);

        Path outGlobPath = new Path(outPath, "clusters-*-final");
        Path clusteredPointsPath = new Path(clusteredPoints);
        System.out.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n", outGlobPath, clusteredPointsPath);

        ClusterDumper clusterDumper = new ClusterDumper(outGlobPath, clusteredPointsPath);
        clusterDumper.printClusters(null);
    }

    public static Job config() throws IOException {
        Job conf = new Job(new Configuration());
        conf.setJobName("KMeansCluster");
        return conf;
    }
}

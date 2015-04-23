package com.chinaunicom.datalabs.mdm.hadoop.boray;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 生成用户基础信息
 * Created by zhangxr103 on 2014/10/30.
 */
public class CountSpeed {
    public static HashMap<String,String> location= Maps.newHashMap();
    public static String location_file="site_list";
    private static double EARTH_RADIUS = 6378.137;//地球半径
    public static SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static double rad(double d)
    {
        return d * Math.PI / 180;
    }

    public static double GetDistance( double lng1,double lat1,  double lng2,double lat2){
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);

        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) +
                Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
        s = s * EARTH_RADIUS;
        s = Math.round(s * 10000) / 10000;
        return s;
    }


    public static class MoveOnce implements Comparable{
        private Date time=null;
        private String from =null;
        private String to =null;
        public MoveOnce(Date time,String start,String to){
            this.time=time;
            this.from =start;
            this.to = to;
        }

        public double getDistance(){
            if(location.containsKey(from)&&location.containsKey(to)){
                String []loc_1=location.get(from).trim().split("\\t");
                double lng1=Double.parseDouble(loc_1[0]);
                double lat1=Double.parseDouble(loc_1[1]);

                String []loc_2=location.get(to).trim().split("\\t");
                double lng2=Double.parseDouble(loc_2[0]);
                double lat2=Double.parseDouble(loc_2[1]);

                return GetDistance(lng1,lat1,lng2,lat2);
            }
            return -1;
        }

        @Override
        public int compareTo(Object o) {
            if (o instanceof MoveOnce){
                return this.time.compareTo(((MoveOnce) o).time);
            }else{
                return -1;
            }
        }

        @Override
        public boolean equals(Object o) {
            return o!=null&&o instanceof MoveOnce&&compareTo(o)==0;
        }

        @Override
        public int hashCode() {
            int result = time.hashCode();
            result = 31 * result + from.hashCode();
            result = 31 * result + to.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return format.format(time) +"\t" + from  +"\t" + to ;
        }
    }

    public static class MapWork extends Mapper<Object, Text, LongWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String ss=value.toString().trim();
            LongWritable key_word=new LongWritable(Long.parseLong(ss.substring(0,ss.indexOf("\t"))));
            context.write(key_word,new Text(ss.substring(ss.indexOf("\t")+1)));
        }
    }

    public static class ReduceWork extends Reducer<LongWritable,Text, LongWritable, Text>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
                    .getConfiguration());
            File file=new File(cacheFiles[0].toString());
            BufferedReader reader=new BufferedReader(new FileReader(file));
            String temp;
            while ((temp=reader.readLine())!=null){
                String []ss=temp.split("\\s");
                location.put(ss[0].trim(),ss[1].trim()+"\t"+ss[2].trim());
            }
        }

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            TreeSet<MoveOnce> set= Sets.newTreeSet();
            for(Text val:values){
                String value=val.toString();
                set.addAll(createMoveOnce(value.split(",")));
            }

            String word="";
            boolean tag=true;
            for(MoveOnce o:set){
                if(tag){
                    word+=o.toString();
                    tag=false;
                }else{
                    word+=","+o.toString();
                }
            }
            context.write(key,new Text(word));
        }

        private List<MoveOnce> createMoveOnce(String[] once) {
            List<MoveOnce> list= Lists.newArrayList();

            for(String s:once){
                String[]ss=s.split("\\t");
                try {
                    list.add(new MoveOnce(format.parse(ss[0]),ss[1],ss[2]));
                } catch (ParseException e) {
                    //TODO nothing
                }
            }
            return list;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser parser=new GenericOptionsParser(conf, args);
        String[] otherArgs = parser.getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: hadoop jar mdm.jar com.chinaunicom.datalabs.mdm.hadoop.boray.TraceMap -file site_list <in> <out>");
            System.exit(2);
        }
        System.err.println(Arrays.toString(otherArgs));
        Job job = new Job(conf, "merge & order in a row");
        job.setJarByClass(CountSpeed.class);
        job.setMapperClass(MapWork.class);
        job.setCombinerClass(ReduceWork.class);
        job.setReducerClass(ReduceWork.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        DistributedCache.createSymlink(job.getConfiguration());//
        try {
            DistributedCache.addCacheFile(new URI("/input/site_list"), job.getConfiguration());
        } catch (URISyntaxException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

package com.chinaunicom.datalabs.mdm.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 *
 * Created by mo on 2014/11/31.
 */
public class AggreateTask {

    static class LineRecord implements WritableComparable<LineRecord> {

        public long upstream_flow=0l,
        downstream_flow=0l,
        total_stream=0l,
        total_time=0l,count=0l;


        public void add(long up_flow,long down_flow,long total_flow,long time,long total_count){
            upstream_flow+=up_flow;
            downstream_flow+=down_flow;
            total_stream+=total_flow;
            total_time+=time;
            count+=total_count;
        }

        public void add(LineRecord record){
            add(record.upstream_flow,record.downstream_flow,record.total_stream,record.total_time,record.count);
        }


        @Override
        public int compareTo(LineRecord o) {
            long thisValue = this.total_stream;
            long thatValue = o.total_stream;
            return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(upstream_flow);
            out.writeLong(downstream_flow);
            out.writeLong(total_stream);
            out.writeLong(total_time);
            out.writeLong(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.upstream_flow=in.readLong();
            this.downstream_flow=in.readLong();
            this.total_stream=in.readLong();
            this.total_time=in.readLong();
            this.count=in.readLong();
        }

        @Override
        public String toString() {
            return "LineRecord{" +
                    "upstream_flow=" + upstream_flow +
                    ", downstream_flow=" + downstream_flow +
                    ", total_stream=" + total_stream +
                    ", total_time=" + total_time +
                    ", count=" + count +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LineRecord that = (LineRecord) o;

            if (count != that.count) return false;
            if (downstream_flow != that.downstream_flow) return false;
            if (total_stream != that.total_stream) return false;
            if (total_time != that.total_time) return false;
            if (upstream_flow != that.upstream_flow) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (upstream_flow ^ (upstream_flow >>> 32));
            result = 31 * result + (int) (downstream_flow ^ (downstream_flow >>> 32));
            result = 31 * result + (int) (total_stream ^ (total_stream >>> 32));
            result = 31 * result + (int) (total_time ^ (total_time >>> 32));
            result = 31 * result + (int) (count ^ (count >>> 32));
            return result;
        }
    }


    public static class MapWork
            extends Mapper<Object, Text, Text, LineRecord> {
        private Text keyText=new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String ss[]=value.toString().trim().split(",");
            if(ss.length!=11){
                return;
            }
            int item_code = Integer.parseInt(ss[0]);
            String item_name = ss[1];
            String client =ss[2];

            long up = Long.parseLong(ss[3]);
            long down = Long.parseLong(ss[4]);
            long flow =Long.parseLong(ss[5]);
            long time = Long.parseLong(ss[6]);
            long count = Long.parseLong(ss[7]);

            int provice_id= Integer.parseInt(ss[8]);
            long id = Long.parseLong(ss[9]);
            String busi_type = ss[10];

            keyText.set(item_code+","+id+","+provice_id+","+busi_type+","+client+","+item_name);
            LineRecord record=new LineRecord();
            record.add(up,down,flow,time,count);
            context.write(keyText,record);
        }
    }

    public static class ReduceWork
            extends Reducer<Text, LineRecord, Text, LineRecord> {

        public void reduce(Text key, Iterable<LineRecord> values,
                           Context context
        ) throws IOException, InterruptedException {
            LineRecord record=new LineRecord();

            for (LineRecord val : values) {
                record.add(val);
            }

            context.write(key, record);
        }
    }

    public static class ParitionWork extends Partitioner<Text,LineRecord>{

        @Override
        public int getPartition(Text text, LineRecord record, int i) {
            StringTokenizer itr = new StringTokenizer(text.toString(),",");
            int item_code=Integer.parseInt(itr.nextToken());
            return item_code%20;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: aggreagate value <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "aggreagate work");
        job.setJarByClass(OriginManage.class);
        job.setMapperClass(MapWork.class);
        job.setPartitionerClass(ParitionWork.class);
        job.setCombinerClass(ReduceWork.class);
        job.setReducerClass(ReduceWork.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LineRecord.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LineRecord.class);
        job.setNumReduceTasks(20);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

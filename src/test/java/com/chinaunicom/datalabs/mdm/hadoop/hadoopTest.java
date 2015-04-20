package com.chinaunicom.datalabs.mdm.hadoop;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Test;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *
 * Created by zhangxr103 on 2015/3/30.
 */
public class hadoopTest {
    private static double EARTH_RADIUS = 6378.137;//地球半径
    public static SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Test
    public void testInputFormat(){
        TextInputFormat tif=new TextInputFormat();
        Configuration conf =new Configuration();

        System.out.println(conf.get("textinputformat.record.delimiter"));
    }

    @Test
    public void test(){
        String []temp="1012071111320030\t0204\t工具软件\t26\t197018\t122\tW\t30AAAAAA\n".split("\\t");
        for(String ss:temp){
            System.out.print(ss+"\t");
        }
    }


    @Test
    public void test1(){
        String temp="02040804,生活综合,C,27634,89310,116944,1267,11,011,1112071203399790,30AAAAAA\n";
        String ss[]=temp.toString().trim().split(",");

        System.out.println(new Date("2014-10-12 07:35:12"));

    }

    @Test
    public void test2(){
        String temp="3481489711234338\t2014-10-12 07:35:12\tWBJ00429\tWBJ00518";
        String ss=temp.substring(0, temp.indexOf("\t"));

        System.out.println(temp.substring(0,temp.indexOf("\t"))+"===="+temp.indexOf("\t"));

    }

    @Test
    public void test3() throws IOException {

//        System.out.println(GetDistance(116.41667,39.91667,121.43333,34.50000));
        File file=new File("H:/000000_0");
        BufferedReader reader=new BufferedReader(new FileReader(file));
        String temp;
        while ((temp=reader.readLine())!=null){
            String []ss=temp.split(new String(new byte[]{1},"GB2312"));
            System.out.println(Arrays.toString(ss));
        }
    }

    private static double rad(double d)
    {
        return d * Math.PI / 180;
    }

    public static double GetDistance( double lng1,double lat1,  double lng2,double lat2)
    {
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

//    @Test
//    public void test4() throws IOException {
//        File file=new File("H:/trace");
//        BufferedReader reader=new BufferedReader(new FileReader(file));
//        String temp;
//        HashMap<LongWritable,Text> map= Maps.newHashMap();
//        while ((temp=reader.readLine())!=null){
//            LongWritable key_word=new LongWritable(Long.parseLong(temp.substring(0,temp.indexOf("\t"))));
//            if(map.containsKey(key_word)){
//                String values=map.get(key_word).toString();
//                TreeSet<MergeAndOrder.MoveOnce> set= Sets.newTreeSet();
//                set.addAll(createMoveOnce(values.split(",")));
//                set.addAll(createMoveOnce(temp.substring(temp.indexOf("\t") + 1).split(",")));
//
//                String word="";
//                boolean tag=true;
//                for(MergeAndOrder.MoveOnce o:set){
//                    if(tag){
//                        word+=o.toString();
//                        tag=false;
//                    }else{
//                        word+=","+o.toString();
//                    }
//                }
//                map.put(key_word,new Text(word));
//            }else{
//                map.put(key_word, new Text(temp.substring(temp.indexOf("\t") + 1)));
//            }
//
//        }
//
//        System.out.println(map);
//
//    }
//
//
//    private static List<MergeAndOrder.MoveOnce> createMoveOnce(String[] once) {
//        List<MergeAndOrder.MoveOnce> list= Lists.newArrayList();
//
//        for(String s:once){
//            String[]ss=s.split("\\t");
//            try {
//                list.add(new MergeAndOrder.MoveOnce(format.parse(ss[0]),ss[1],ss[2]));
//            } catch (ParseException e) {
//                //TODO nothing
//            }
//        }
//        return list;
//    }


}

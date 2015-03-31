package com.chinaunicom.datalabs.mdm.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Test;

import java.util.StringTokenizer;

/**
 *
 * Created by zhangxr103 on 2015/3/30.
 */
public class hadoopTest {

    @Test
    public void testInputFormat(){
        TextInputFormat tif=new TextInputFormat();
        Configuration conf =new Configuration();

        System.out.println(conf.get("textinputformat.record.delimiter"));
    }

    @Test
    public void test(){
        String []temp="3481489715844665,,2,19871205,20111104,11,185,73502,Android 4.2,1,30AAAAAA,99002138,03,0,0,,01,0,125.2,470,3931".split(",");
        for(String ss:temp){
            System.out.print(ss+"\t");
        }
    }


    @Test
    public void test1(){
        String temp="02040804,生活综合,C,27634,89310,116944,1267,11,011,1112071203399790,30AAAAAA\n";
        String ss[]=temp.toString().trim().split(",");

        System.out.println(ss.length);

    }
}

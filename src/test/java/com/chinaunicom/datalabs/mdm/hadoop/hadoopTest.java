package com.chinaunicom.datalabs.mdm.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Test;

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
}

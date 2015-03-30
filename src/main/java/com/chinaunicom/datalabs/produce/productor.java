package com.chinaunicom.datalabs.produce;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Random;

/**
 * product artifact data
 * Created by xinrong on 2015/3/29.
 */
public class productor {

    public static Random rd=new Random();

    public static int timeRange=30;//一个月的通话信息
    public static int areaRange=100;// 100个基站
    public static int phoneRange=10000;


    public static long baseNumber=18666880000l;


    public static void main(String...args) throws IOException {
        long count=0;
        for(int i=0;i<phoneRange;i++){
            count+=commit_record(i+baseNumber);
        }
        System.out.println("total number:"+count);
    }

    private static long commit_record(long phone_number) throws IOException {
        long count = 0l;
        BufferedWriter bw=new BufferedWriter(new FileWriter("./call_detail",true));
        long [] frequent_user=new long[GRandom(10)+5];//每个人至少要有5个人以上的常用联系人，高斯分布，均值15个
        initReciever(frequent_user);//初始化该手机用户的常用联系人

        //人们总是在两个地方打电话，家里和办公室,平均分布
        int shome_locate=rd.nextInt(areaRange);
        int swork_locate=rd.nextInt(areaRange);

        int rhome_locate=rd.nextInt(areaRange);
        int rwork_locate=rd.nextInt(areaRange);

        Calendar cal=Calendar.getInstance();
        //每个常用联系人在每天都可能发生的通话次数和时长
        for(int i=0;i<timeRange;i++){
            cal.add(Calendar.DAY_OF_MONTH,1);
            for(long reciever:frequent_user) {
                int calls = GRandom(3);//一天内与一个人的通话次数平均3次
                while (calls-- > 0){
                    recordCalls(bw, phone_number, reciever, cal.getTimeInMillis(), GRandom(25), rd.nextInt(2) < 1 ? shome_locate : swork_locate,rd.nextInt(2) < 1 ? rhome_locate : rwork_locate);
                    count++;
                }
            }
            bw.flush();
        }
        return count;
    }

    /**
     * 高斯分布产生正的随机数
     * @param mean 随机数的平均值
     * @return 整数
     */
    public static int GRandom(int mean){
        double seed=rd.nextGaussian();
        int number= (int) ((seed+3)*mean/3);
        return number>0?number:0;
    }

    private static void recordCalls(BufferedWriter bw, long phone_number, long reciever, long daySeq, int duration, int send_locate,int reciece_locate) throws IOException {
        String line=phone_number + "\t" + reciever + "\t" + daySeq + "\t" + duration + "\t" + send_locate+"\t"+reciece_locate;
//        System.out.println(line);
        bw.append(line + "\n");
    }

    private static void initReciever(long[] reciever) {
        for(int i=0;i< reciever.length;i++){
            reciever[i]=rd.nextInt(phoneRange)+baseNumber;
        }
    }

}

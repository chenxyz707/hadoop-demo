package com.chenxyz.hadoop.demo.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by chenxyz on 2018/11/14.
 */
public class MyPartition extends Partitioner<Object, IntWritable> {

    //key、value分别指的是Mapper任务的输出，numReduceTasks指的是设置的Reducer任务数量
    @Override
    public int getPartition(Object key, IntWritable value, int numPartitions) {
        final String ke = key.toString();
        if ("男".equals(ke)) {
            return 0;//% numPartitions ;//此时numPartitions =2，因为我们要设置2个分区

        } else if ("女".equals(ke)) {
            return 1;//% numPartitions;
        } else {
            return 2;//% numPartitions;
        }
    }
}

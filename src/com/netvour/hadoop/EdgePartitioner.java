package com.netvour.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * 实现自己的Partitioner，默认是会按对象的哈希值进行分区
 * 要实现相同的出发点分到相同的reducer则要改写成如下
 * @author zzjie
 *
 */
public class EdgePartitioner implements Partitioner<Edge, Writable>
{
	/**
	 * 返回一个0至reduce任务数之间的整数
	 */
    @Override
    public int getPartition(Edge key, Writable value, int numPartitions)
    {
        return key.getDepartureNode().hashCode() % numPartitions;
    }
    @Override
    public void configure(JobConf conf) { }
}
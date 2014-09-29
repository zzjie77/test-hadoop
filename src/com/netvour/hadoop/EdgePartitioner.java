package com.netvour.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * ʵ���Լ���Partitioner��Ĭ���ǻᰴ����Ĺ�ϣֵ���з���
 * Ҫʵ����ͬ�ĳ�����ֵ���ͬ��reducer��Ҫ��д������
 * @author zzjie
 *
 */
public class EdgePartitioner implements Partitioner<Edge, Writable>
{
	/**
	 * ����һ��0��reduce������֮�������
	 */
    @Override
    public int getPartition(Edge key, Writable value, int numPartitions)
    {
        return key.getDepartureNode().hashCode() % numPartitions;
    }
    @Override
    public void configure(JobConf conf) { }
}
package com.netvour.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * ��һ��Ŀ¼���ļ��ڱ��浽hdfs�Ĺ����кϲ���һ���ļ�
 * �������Ⱥϲ��ٱ��浽hdfs
 * @author zzjie
 * ����ֱ����eclipse run on hadoop������eclipseװ��master�ϣ�����Ҫ��jar��scp��mater������
 * ����ǰҪ��jar����Ŀ¼����inputĿ¼�����������ɸ��ļ�
 * hadoop jar test-hadoop.jar putmerge input outputFile 
 */
public class PutMerge {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: putmerge <in> <out>");
	      System.exit(2);
	    }
		
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		
		// 1. ָ������Ŀ¼������ļ�
		Path inputDir = new Path(otherArgs[0]); 
		Path hdfsFile = new Path(otherArgs[1]);
		
		try {
			// 2. ��ȡһ�������ļ�
			FileStatus[] inputFiles = local.listStatus(inputDir);
			
			// 3. ����hdfs�����     FSDataInputStream�Ǽ̳�jdk��DataOutputStream�����Ǽ����������ȡ
			FSDataOutputStream out = hdfs.create(hdfsFile);
			for (int i = 0; i < inputFiles.length; i++) {
				System.out.println(inputFiles[i].getPath().getName());
				// 4. �򿪱����ļ�������
				FSDataInputStream in = local.open(inputFiles[i].getPath());
				byte[] buffer = new byte[256];
				int bytesRead = 0;
				while((bytesRead = in.read(buffer)) != -1) {
					out.write(buffer, 0 , bytesRead);
				}
				in.close();
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

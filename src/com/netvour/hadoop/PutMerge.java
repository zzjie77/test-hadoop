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
 * 将一个目录的文件在保存到hdfs的过程中合并成一个文件 而不是先合并再保存到hdfs
 * 
 * @author zzjie 不能直接在eclipse run on hadoop，除非eclipse装在master上，否则要成jar包scp到mater上运行 运行前要在jar所在目录创建input目录，并创建若干个文件
 *         hadoop jar test-hadoop.jar putmerge input outputFile
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

		// 1. 指定输入目录和输出文件
		Path inputDir = new Path(otherArgs[0]);
		Path hdfsFile = new Path(otherArgs[1]);

		try {
			// 2. 获取一组输入文件
			FileStatus[] inputFiles = local.listStatus(inputDir);

			// 3. 创建hdfs输出流 FSDataInputStream是继承jdk的DataOutputStream，但是加上了随机读取
			FSDataOutputStream out = hdfs.create(hdfsFile);
			for (int i = 0; i < inputFiles.length; i++) {
				System.out.println(inputFiles[i].getPath().getName());
				// 4. 打开本地文件输入流
				FSDataInputStream in = local.open(inputFiles[i].getPath());
				byte[] buffer = new byte[256];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) != -1) {
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

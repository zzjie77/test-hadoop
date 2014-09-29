package com.netvour.hadoop;

import org.apache.hadoop.util.ProgramDriver;

/**
 * 运行入口
 */
public class ExampleDriver {

	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("putmerge", PutMerge.class, "合并一个目录里的文件");
			pgd.addClass("wordcount", WordCount.class, "A map/reduce program that counts the words in the input files.");
			exitCode = pgd.run(argv);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}
}

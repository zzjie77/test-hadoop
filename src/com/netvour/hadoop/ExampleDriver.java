package com.netvour.hadoop;


import org.apache.hadoop.util.ProgramDriver;

/**
 * �������
 */
public class ExampleDriver {
  
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("putmerge", PutMerge.class, 
                "�ϲ�һ��Ŀ¼����ļ�");	
      pgd.addClass("wordcount", WordCount.class, 
                   "A map/reduce program that counts the words in the input files.");
      exitCode = pgd.run(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
	

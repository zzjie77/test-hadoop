package com.netvour.hadoop;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.Random;

public class GenerateData {

	public static void main(String[] args) throws Exception {
		/**
		 * 换机的号码|号码品牌|归属地市|原IMEI号|新IEMI号 
		 * 15914865711|3|GZ|863999021756480|453999021756480 
		 * 13614865712|3|BJ|456999021756480|235999021756480
		 * 13614865712|3|BJ|456999021756480|235999021756480 
		 * 13614865712|3|BJ|456999021756480|235999021756480 
		 * 13614865712|3|BJ|456999021756480|235999021756480
		 * 13614865712|3|BJ|456999021756480|235999021756480
		 */
		Random r = new Random();
		DecimalFormat df = new DecimalFormat("00");
		PrintWriter out = new PrintWriter(
						  	new BufferedOutputStream(
							  new FileOutputStream("changeCard.dat"),2048
							)
						  );
		for (int i = 0; i < 30; i++) {
			String mobile = "159148657" + df.format(r.nextInt(50));
			String oldIMEI = "4569990217564" + df.format(r.nextInt(10));
			String newIMEI = "3219990217564" + df.format(r.nextInt(10));

			String line = mobile + "|3|GZ|" + oldIMEI + "|" + newIMEI;
			System.out.println(line);
			out.println(line);
		}
		out.close();
		System.out.println("generated!");
	}

}

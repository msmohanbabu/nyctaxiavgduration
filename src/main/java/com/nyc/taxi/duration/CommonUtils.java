//Methods to print RDD , Dataframe and hold spark connection to see the spark jobs @locahost:pppp
package com.nyc.taxi.duration;

/****
 * 
 *  @author Mohan MS
 * 
 */

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CommonUtils {

	public static String dateFormat = "yyyy-MM-dd HH:mm:ss";

	public static void hold() {
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void printStringRDD(JavaRDD<Row> stringRDD, int count) {

		for (Row s : stringRDD.take(count)) {
			System.out.println(s);
		}
		System.out
				.println("-----------------------------------------------------------------------------------");
	}

	public static void printDataframe(Dataset<Row> datasetDF, int count) {

		datasetDF
				.foreach((ForeachFunction<Row>) row -> System.out.println(row));
		System.out
				.println("-----------------------------------------------------------------------------------");
	}
}

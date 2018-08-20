//Methods to print RDD , Dataframe and hold spark connection to see the spark jobs @locahost:4040
package com.nyc.taxi.duration;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CommonUtils {
	
	
	public static void hold() {
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void printStringRDD(JavaRDD<String> stringRDD, int count) {
		
		for ( String s : stringRDD.take(count)) {
			System.out.println(s);
		}
		System.out.println("-----------------------------------------------------------------------------------");
	}

	public static void printDataframe(Dataset<Row> datasetDF, int count) {
		
		datasetDF.foreach((ForeachFunction<Row>) row -> System.out.println(row));
		System.out.println("-----------------------------------------------------------------------------------");
	}
}

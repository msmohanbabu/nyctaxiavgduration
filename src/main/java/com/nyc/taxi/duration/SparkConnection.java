package com.nyc.taxi.duration;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession ;

import java.util.List;

import org.apache.spark.SparkConf;

/***
 * 
 * @author Mohan MS
 *
 */

public class SparkConnection {

	private static String appName = "AvgDuration";
	private static String sparkMaster = "local[2]";

	private static JavaSparkContext spContext = null;
	private static SparkSession sparkSession = null;
	private static String tempDir = "file:////home//cloudera//spark";

	private static void getConnection() {

		if (spContext == null) {
			// Setup Spark configuration
			SparkConf conf = new SparkConf().setAppName(appName).setMaster(
					sparkMaster);

			System.setProperty("hadoop.home.dir", "//usr//lib//hadoop");

			spContext = new JavaSparkContext(conf);

			// Create Spark Session from configuration
			sparkSession = SparkSession.builder().appName(appName)
					.master(sparkMaster)
					.config("spark.sql.warehouse.dir", tempDir).getOrCreate();

		}

	}

	public static JavaSparkContext getContext() {
		if (spContext == null) {
			getConnection();
		}
		return spContext;
	}

	public static SparkSession getSession() {
		if (sparkSession == null) {
			getConnection();
		}
		return sparkSession;
	}

}

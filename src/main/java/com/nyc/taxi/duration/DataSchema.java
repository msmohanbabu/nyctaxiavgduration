//DataSchemas
package com.nyc.taxi.duration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/***
 * 
 * @author Mohan MS
 *
 */

public class DataSchema {

/***
 * Original Schema
 */
	private static StructType nycSchema = DataTypes
			.createStructType(new StructField[] {
					DataTypes.createStructField("VENDOR ID",
							DataTypes.StringType, false),
					DataTypes.createStructField("PICKTIME",
							DataTypes.StringType, false),
					DataTypes.createStructField("DROPTIME",
							DataTypes.StringType, false),
					DataTypes.createStructField("PASSCOUNT",
							DataTypes.StringType, false),
					DataTypes.createStructField("DISTANCE",
							DataTypes.StringType, false),
					DataTypes.createStructField("RATECODE",
							DataTypes.StringType, false),
					DataTypes.createStructField("STRFWDFLAG",
							DataTypes.StringType, false),
					DataTypes.createStructField("PICKUPLOCATION",
							DataTypes.StringType, false),
					DataTypes.createStructField("DROPLOCATION",
							DataTypes.StringType, false),
					DataTypes.createStructField("PAYTYPE",
							DataTypes.StringType, false),
					DataTypes.createStructField("FARE", DataTypes.StringType,
							false),
					DataTypes.createStructField("EXTRA", DataTypes.StringType,
							false),
					DataTypes.createStructField("TAX", DataTypes.StringType,
							false),
					DataTypes.createStructField("TIPAMOUNT",
							DataTypes.StringType, false),
					DataTypes.createStructField("TOLL", DataTypes.StringType,
							false),
					DataTypes.createStructField("SURCHARGE",
							DataTypes.StringType, false),
					DataTypes.createStructField("TOTAL", DataTypes.StringType,
							false) });

/***
 * Final Schema
 */
	private static StructType finalSchema = DataTypes
			.createStructType(new StructField[] {
					DataTypes.createStructField("PICKUPLOCATION",
							DataTypes.StringType, false),
					DataTypes.createStructField("DURATION",
							DataTypes.StringType, false) });

/***
 * Schema to transform and filter RDD 
 */
	private static StructType modifiedSchema = nycSchema.add("ERROR_COLUMN",
			DataTypes.StringType, true).add("ERROR_VALUE",
			DataTypes.StringType, true);

	public static StructType getModifiedSchema() {
		return modifiedSchema;
	}

	public static StructType getNycSchema() {
		return nycSchema;
	}

	public static StructType getFinalSchema() {
		return finalSchema;
	}

	public static JavaRDD<Row> rowToRddPartition(Dataset<Row> rawSchema,
			int numPartition) {
		return (rawSchema.toJavaRDD().repartition(numPartition));
	}

	public static JavaRDD<Row> rowToRdd(Dataset<Row> rawSchema) {
		return (rawSchema.toJavaRDD());
	}
/*
 * 
 * @param timeStampToValidate
 * @return
 */

	public static boolean isTimeStampValid(String timeStampToValidate) {
		if (timeStampToValidate == null || timeStampToValidate == "") {
			return false;
		}
		try {
			new SimpleDateFormat(CommonUtils.dateFormat)
					.parse(timeStampToValidate);
			return true;
		} catch (ParseException e) {
			return false;

		}

	}

}

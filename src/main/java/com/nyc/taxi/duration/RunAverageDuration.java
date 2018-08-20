package com.nyc.taxi.duration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;

import scala.Serializable;
import static org.apache.spark.sql.functions.*;

/***
 * 
 * @author Mohan MS
 *
 */

public class RunAverageDuration implements Serializable {

	private Date pickUpTimestamp;
	private Date dropTimestamp;
	private JavaRDD<Row> taxiRDD;
	private Dataset<Row> taxiOrigData;

	public Date getPickUpTimestamp() {
		return pickUpTimestamp;
	}

	public Date getDropTimestamp() {
		return dropTimestamp;
	}

/****
 * @param  Dataset<Row> 
 * @return JavaRDD<Row>
 * @throws NoSuchFieldException
 * @throws IllegalArgumentException
 * @throws ParseException
 * Select the data needed for average calculation
 */
	public static JavaRDD<Row> selectData(Dataset<Row> origData)
			throws NoSuchFieldException, IllegalArgumentException,
			ParseException {

		JavaRDD<Row> filterRDD = origData.toJavaRDD().map(
				new Function<Row, Row>() {

					@Override
					public Row call(Row record) throws Exception {
						Row filteredRow = RowFactory.create(
								String.valueOf(record.getAs("PICKUPLOCATION")), 
								String.valueOf(
										 new SimpleDateFormat(CommonUtils.dateFormat)
										     .parse(record.getAs("DROPTIME"))
										     .getTime() -
										 new SimpleDateFormat(CommonUtils.dateFormat)
										     .parse(record.getAs("PICKTIME"))
											 .getTime()));
						return filteredRow;

					}
				});

		return filterRDD;
	}
/****
 * 
 * @param Dataset<Row>
 * @return JavaRDD<Row> 
 * Filter the Dataframe - Data Cleansing - Validate the data needed for processing
 */

	public static JavaRDD<Row> filterDataFrame(Dataset<Row> modifiedDF) {
		JavaRDD<Row> taxiRDDModified = modifiedDF.toJavaRDD().map(
				new Function<Row, Row>() {

					@Override
					public Row call(Row record) throws Exception {

						String columnError = "YES";
						String columnValue = null;

						if (DataSchema.isTimeStampValid(String.valueOf(record
								       .getAs("PICKTIME"))) == false) {
							columnError = "NO";
							columnValue = String.valueOf(record.getAs("PICKTIME"));
						}
						if (DataSchema.isTimeStampValid(String.valueOf(record
								       .getAs("DROPTIME"))) == false) {
							columnError = "NO";
							columnValue = String.valueOf(record.getAs("DROPTIME"));
						}
						if (record.getAs("PICKUPLOCATION") == null
								|| record.getAs("PICKUPLOCATION") == "") {
							columnError = "NO";
							columnValue = String.valueOf(record.getAs("PICKUPLOCATION"));
						}
						
						Row retRecord = RowFactory.create(
								String.valueOf(record.getString(0)),
								String.valueOf(record.getString(1)),
								String.valueOf(record.getString(2)),
								String.valueOf(record.getString(3)),
								String.valueOf(record.getString(4)),
								String.valueOf(record.getString(5)),
								String.valueOf(record.getString(6)),
								String.valueOf(record.getString(7)),
								String.valueOf(record.getString(8)),
								String.valueOf(record.getString(9)),
								String.valueOf(record.getString(10)),
								String.valueOf(record.getString(11)),
								String.valueOf(record.getString(12)),
								String.valueOf(record.getString(13)),
								String.valueOf(record.getString(14)),
								String.valueOf(record.getString(15)),
								String.valueOf(record.getString(16)),
								String.valueOf(columnError),
								String.valueOf(columnValue));

						return retRecord;
					}
				});
		return taxiRDDModified;
	}
/**
 * 
 * @param Dataset<Row>
 * @return Dataset<Row>
 * @throws NoSuchFieldException
 * Calculate Average using Spark class Function.avg
 */
	
	public static Dataset<Row> AverageCalculation(Dataset<Row> filterDF)
			throws NoSuchFieldException {

		Dataset<Row> finalData = filterDF.groupBy("PICKUPLOCATION")
				.agg(functions.avg(filterDF.col("DURATION").as("AVERAGE(ms)")))
				.orderBy("PICKUPLOCATION");

		return finalData;

	}

/***
 * 
 * @param Dataset<Row>
 * @param OutputPath 
 * @throws AnalysisException
 */

	public static void writeAsCSV(Dataset<Row> finalDF, String OutCSV)
			throws AnalysisException {

		finalDF.coalesce(1).write().option("header", true).csv(OutCSV);

	}

}

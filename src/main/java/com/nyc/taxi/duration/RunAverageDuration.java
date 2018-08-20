//Processing the data
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

import scala.Serializable;
import static org.apache.spark.sql.functions.*;


public class RunAverageDuration implements Serializable {

	private static SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
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

	

	public static JavaRDD<Row> selectData(Dataset<Row> origData)
			throws NoSuchFieldException, IllegalArgumentException,
			ParseException {

		JavaRDD<Row> filterRDD = origData.toJavaRDD().map(new Function<Row, Row>() {

			@Override
			public Row call(Row record) throws Exception {
				Row filteredRow = RowFactory.create(String.valueOf(record
						.<String> getAs("PICKUPLOCATION")), Long
						.valueOf(dateFormat.parse(record.getAs("DROPTIME"))
								.getTime()
								- dateFormat.parse(record.getAs("PICKTIME"))
										.getTime()));
				return filteredRow;

			}
		});

		return filterRDD;
	}

//	AverageCalculation - Calculate 
	public static Dataset<Row> AverageCalculation(Dataset<Row> filterDF)
			throws NoSuchFieldException {

		Dataset<Row> finalData = filterDF.groupBy("PICKUPLOCATION")
				.agg(avg(filterDF.col("DURATION").as("AVERAGE")))
				.orderBy("PICKUPLOCATION");

		return finalData;

	}

	public static void writeAsCSV(Dataset<Row> finalDF, String OutCSV)
			throws AnalysisException {

		finalDF.coalesce(1).write().option("header", true).csv(OutCSV);

	}

	public static JavaRDD<Row> filterDataFrame(Dataset<Row> modifiedDF) {
		JavaRDD<Row> taxiRDDModified = modifiedDF.toJavaRDD().map(
				new Function<Row, Row>() {

					@Override
					public Row call(Row record) throws Exception {

						String columnError = "VALID";
						String columnValue = null;

						if (!DataSchema.isTimeStampValid(record
								.<String> getAs("PICKTIME"))) {
							columnError = null;
							columnValue = record.<String> getAs("PICKTIME");
						}
						if (!DataSchema.isTimeStampValid(record
								.<String> getAs("DROPTIME"))) {
							columnError = null;
							columnValue = record.<String> getAs("PICKTIME");
						}
						if (record.<String> getAs("PICKUPLOCATION") == null
								|| record.<String> getAs("PICKUPLOCATION") == "") {
							columnError = null;
							columnValue = record
									.<String> getAs("PICKUPLOCATION");
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

}

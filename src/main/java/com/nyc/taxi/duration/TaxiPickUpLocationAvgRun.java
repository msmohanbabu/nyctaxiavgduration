package com.nyc.taxi.duration;

import java.io.FileNotFoundException;
import java.time.format.DateTimeParseException;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.RelationalGroupedDataset;
import com.nyc.taxi.duration.SparkConnection;

/****
 * 
 * @author Mohan MS
 *
 */

public class TaxiPickUpLocationAvgRun implements scala.Serializable {

	// private static String INPUT_PATH_FILE =
	// "hdfs://192.168.0.12:8020/user/cloudera/yellow_tripdata_2017-01.csv";
	// private static String OUTPUT_PATH_FILE =
	// "hdfs://192.168.0.12:8020/user/cloudera/output2";
	private static String INPUT_PATH_FILE = null;
	private static String OUTPUT_PATH_FILE = null;

	public static void main(String[] args) {

		try {

			if (args.length == 2) {
				INPUT_PATH_FILE = args[0];
				OUTPUT_PATH_FILE = args[1];
			} else {
				System.out
				.println("Invalid number Arguments - Input and Output file with path needs to be passed as arguments.");
				throw new FileNotFoundException();
			}

			SparkSession spSession = SparkConnection.getSession();

			// Load Data from HDFS
			Dataset<Row> taxiRawCSV = spSession.read().option("header", true)
					.schema(DataSchema.getNycSchema())
					.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
					.csv(INPUT_PATH_FILE);
			
            /*
			Dataset<Row>[] taxiRawDataSplit = taxiRawCSV
			                                   .randomSplit(new double[] {0.9,0.1});
			Dataset<Row> testData = taxiRawDataSplit[1];
           */

			// Creating a Dataframe for Cleansing
			Dataset<Row> transformedData = spSession.createDataFrame(
					RunAverageDuration.filterDataFrame(taxiRawCSV),
					DataSchema.getModifiedSchema());
			
			transformedData.cache();
			// Cleansing/filter the data. Omitting the data if invalid
			Dataset<Row> filteredData = transformedData
					.filter(new FilterFunction<Row>() {
						@Override
						public boolean call(Row row) throws Exception {

							return row.getString(17).equals("YES");
						}
					});

			// Create Dataframe with selected fields
			Dataset<Row> taxiFilteredDF = spSession.createDataFrame(
					RunAverageDuration.selectData(transformedData),
					DataSchema.getFinalSchema());
			
			taxiFilteredDF.cache();
			transformedData.unpersist();

			// Calculate Average
			Dataset<Row> taxiFinalDF = RunAverageDuration
					.AverageCalculation(taxiFilteredDF);
			
			
			//Write as CSV in OUTPUT_PATH_FILE
			RunAverageDuration.writeAsCSV(taxiFinalDF, OUTPUT_PATH_FILE);

			System.out.println("Processing has been completed. "
				             	+ "Please verify the output in "
							    + OUTPUT_PATH_FILE);
			
		} catch (IllegalArgumentException | NoSuchFieldException
				| SecurityException e) {
			e.printStackTrace();
		} catch (DateTimeParseException | AnalysisException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			
		}
	}
}


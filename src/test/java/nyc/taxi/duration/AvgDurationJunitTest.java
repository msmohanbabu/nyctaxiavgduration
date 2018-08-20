package nyc.taxi.duration;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import com.nyc.taxi.duration.DataSchema;
import com.nyc.taxi.duration.RunAverageDuration;


//@SparkTest
public class AvgDurationJunitTest {

	public SparkSession Spark;
	private Dataset<Row> travel;
	
	@BeforeEach
	public void before() {
		List<Row> rows = Arrays.asList(
				RowFactory.create("123", "2018-05-18 12:05:02", "2018-05-18 12:20:02"), 
				RowFactory.create("123", "2018-05-17 11:05:02", "2018-05-18 11:20:02"),
				RowFactory.create("456", "2018-05-16 10:05:02",	"2018-05-16 10:20:02"), 
				RowFactory.create("456", "2018-05-15 10:05:02", "2018-05-15 10:20:02"),
				RowFactory.create("789", "2018-05-14 09:05:02", "2018-05-14 09:20:02"));

		
		StructType dataschema = DataTypes
				.createStructType(new StructField[] {
						DataTypes.createStructField("LOCATION",
								DataTypes.StringType, false),
						DataTypes.createStructField("PICKTIME",
								DataTypes.TimestampType, false),
						DataTypes.createStructField("DROPTIME",
								DataTypes.TimestampType, false)});
		
						

		Dataset<Row> travel = Spark.createDataFrame(rows, dataschema);
		
		travel.collect(); 
		
		travel.show();
		
	}

	@Test
	public void mustReturnAverage() throws NoSuchFieldException, IllegalArgumentException, ParseException {
		
		before();
		Dataset<Row> selectDataDF = Spark.
				     createDataFrame(RunAverageDuration.selectData(travel),DataSchema.getFinalSchema());
		Dataset<Row> finalData = RunAverageDuration.AverageCalculation(selectDataDF);
		finalData.show();

		Assertions.assertEquals("123",finalData.first().<String> getAs("DURATION"));
	}

}

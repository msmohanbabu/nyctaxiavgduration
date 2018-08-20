"# nyctaxiavgduration"

The main class is com.nyc.taxi.duration.TaxiPickUpLocationAvgRun

Submit using spark2-submit if submitting in terminal.

Input file used here is https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-01.csv. Scheme of the dataset is in header

./bin/spark2-submit --class  com.nyc.taxi.duration.TaxiPickUpLocationAvgRun --master local[2] /<path to maven project>/target/ taxi.duration-0.0.1-SNAPSHOT.jar <hdfs://<hdfs input directory> hdfs://<hdfs output directory> 

com.nyc.taxi.duration.RunAverageDuration - methods for processing the data using RDD/Dataframes/dataset.
com.nyc.taxi.duration.DataSchema  - nycSchema(Original Schema) ,finalSchema(Final Output schema), modifiedSchema - Schema used to filter the data.
com.nyc.taxi.duration.SparkConnection - Spark Session and hadoop directory details.
com.nyc.taxi.duration.AverageTuple - Involved more JavaRDD to calculate average. Please refer SparkKeyvalue.Java for that approach.
                                     In this one, I have used Dataframe/Datasets and functions.avg

Note: Coded and tested in cloudera CDH 5.13

Spark 2.0 and Hadoop 2.6(Cloudera)
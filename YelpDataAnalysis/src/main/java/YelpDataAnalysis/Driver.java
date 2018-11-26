package YelpDataAnalysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.joda.time.LocalDate;

import scala.Tuple2;

public class Driver {

	//for packaging
	private static final Logger logger = Logger.getLogger(Driver.class);
	private static final Pattern COLON = Pattern.compile("\\:");
	private static final String master = "spark://baton-rouge:30317";
	private static final String local = "local";

	public static JavaPairRDD<String, Iterable<String>> parseFile(SparkSession spark, String[] args) {
		JavaRDD<String> web_map = spark.read().textFile(args[0]).javaRDD();

		JavaPairRDD<String, Iterable<String>> links = web_map.mapToPair(s -> {
			String[] parts = {s.substring(0, s.lastIndexOf(":")), s.substring(s.lastIndexOf(":")+1)};
			return new Tuple2<>(parts[0], Arrays.asList(parts[1].trim().split("\t")));
		});
//		for (Tuple2<String, Iterable<String>> o : links.collect()) {
//			System.out.print(o._1() + ": ");
//			for (String s : o._2()) {
//				System.out.print(s + "\t");
//			}
//			System.out.println();
//		}
		return links;	
	}
	
	public static void main(String[] args) {
		System.out.println(getLocalCurrentDate());
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Page Rank")
//				.config("spark.master", master)
				.config("spark.master", local)
				.getOrCreate();
		JavaPairRDD<String, Iterable<String>> data = parseFile(spark, args);
		System.out.println("===========================================================CHECK========================================================");
		System.out.println("================================================== data size: " + data.collect().size() + " ======================================================");
		System.out.println("====================================================================== Begin 3D Clustering ========================================================");
		// run 3DClustering
		Cluster3D.kMeansCluster(data);
	}

	private static String getLocalCurrentDate() {

		if (logger.isDebugEnabled()) {
			logger.debug("getLocalCurrentDate() is executed!");
		}

		LocalDate date = new LocalDate();
		return date.toString();

	}

}

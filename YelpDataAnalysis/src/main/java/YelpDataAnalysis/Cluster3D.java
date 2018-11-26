package YelpDataAnalysis;

import org.apache.spark.api.java.JavaPairRDD;
import org.codehaus.janino.Java;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import scala.Tuple2;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Cluster3D {

    /* The format of data are as follows:

            Business Name ->    State,
                                latitude,
                                longitude,
                                Yelp rating,
                                Number of reviews,
                                Categories

      Required variables:

      int k = number of clusters = 11 (number of cities)
      int s = number of states ( this includes canadian provinces)
      int x = number of random businesses per state
      int totalSize = number of total businesses in the dataset = 142544

      Step1) Define an initial (random) solution as vectors of means.

            m(t=0) = [m1, m2, ..., mk]

            where mk is a vector containing [lat, lon, rating]
            ============================ Random numbers ==================================
            You can randomly grab 11 points using data.take(11)


            ================ Spacing by state (won't work, k < number of states) =================

            It would be wise to space these random vectors out according to state. To do this,
            I shall create a new map...

            State -> Tuple2< Business, Iterable<String>

            ... and do a reduce. This will result in...

            String -> List<Tuple2<String, Iterable<String>>>
              map       key

            ... or something like that. Then I for each state I will need to select
            x amount of random businesses from each state where...

            x = k * (totalSize/key.size())

    Step 2) Classify each input data according to m(t)
        For this, you will associate each business with one cluster most similar to itself.

    Step 3) Use the classification obtained in step 2 to recompute the vectors of means m(t+1)
        For this, you will create

    ================= RETURN VALUE =====================
    kMeansCluster returns a JavaPairRDD<String, Iterable<String>> which contains the following information...

             Business Name ->   Cluster (0 - 10),
                                State,
                                latitude,
                                longitude,
                                Yelp rating,
                                Number of reviews,
                                Categories

    */

    public static JavaPairRDD<String, Iterable<String>> kMeansCluster(JavaPairRDD<String, Iterable<String>> data){
        System.out.println("============================================================================= Print Data ===================================================================================");

        // Step 1)
        // random sampling
//        List<Tuple2<String, Iterable<String>>> sample = randomSample(data, 11);
        List<Tuple2<String, Iterable<String>>> sample = sampleByCity(data);



        // Step 2) classify each business in data according to the business in sample
        // which it is most similar to.
        for(int i = 0; i < 10; i++){
            System.out.println("================================================================== Step 2 ==================================================================");
        }
        JavaPairRDD<String, Iterable<String>> clusters = classify(sample, data, true);
        for(int i = 0; i < 10; i++){
            System.out.println("================================================================== CHECK!!! ==================================================================");
        }
        for(int i = 0; i < 10; i++){
            System.out.println("================================================================================================== " + clusters.collect().get(i)._2.toString() + "===================================================================");
        }

        // Step 3) Use the classification obtained in step 2 to recompute the vectors of means m(t+1)

        List<Tuple2<String, Iterable<String>>> means = getMeans(clusters);

        /* Steps 4 & 5)
            Keep recomputing m(t+1). Add up the total difference in means between m(t) and m(t+1).
            If the total difference in means is less than 0.05, the algorithm has finished
            and the businesses have been sorted into their final cluster.
        */
        double epsilon = 0.001;
        double diff = 3.00;
        while(diff > epsilon){
            List<Tuple2<String, Iterable<String>>> means1 = means;
            clusters = classify(means, clusters, false);
            List<Tuple2<String, Iterable<String>>> means2 = getMeans(clusters);
            double nextDiff = 0;

            for(int i = 0; i < means.size(); i++){
                Tuple2<String, Iterable<String>> mean1 = means1.get(i);
                Tuple2<String, Iterable<String>> mean2 = means2.get(i);
                Iterator<String> iter1 = mean1._2.iterator();
                Iterator<String> iter2 = mean2._2.iterator();
                double lat1 = Double.parseDouble(iter1.next());
                double lon1 = Double.parseDouble(iter1.next());
                double rating1 = Double.parseDouble(iter1.next());
                int count1 = Integer.parseInt(iter1.next());
                double lat2 = Double.parseDouble(iter2.next());
                double lon2 = Double.parseDouble(iter2.next());
                double rating2 = Double.parseDouble(iter2.next());
                int count2 = Integer.parseInt(iter2.next());
                // number of stars*(log(reviews)/log(7968)) where 7968 is the max number of reviews
                lat1 = (lat1 + 90)/180;
                lat2 = (lat2 + 90)/180;
                lon1 = (lon1 + 180)/360;
                lon2 = (lon2 + 180)/360;
                rating1 = rating1 * (Math.log(count1)/Math.log(7968));
                rating2 = rating2 * (Math.log(count1)/Math.log(7968));

                nextDiff += Math.abs(lat1 - lat2);
                nextDiff += Math.abs(lon1 - lon2);
                nextDiff += Math.abs(rating1 - rating2);


            }
            means = means2;
            diff = nextDiff;
        }

        /* means contains...
                Cluster_i ->    lat
                                lon
                                rating
                                num

         */

        printClusters(clusters);

        return clusters;

    }

    private static void printClusters(JavaPairRDD<String, Iterable<String>> clusters){
        List<Tuple2<String, Iterable<String>>> list = clusters.collect();
        for(int i = 0; i < list.size(); i++){
            System.out.print(list.get(i)._1 + ":");
            for(String s : list.get(i)._2){
                System.out.print("\t" + s);
            }
            System.out.print("\n");
        }
    }

	public static List<Tuple2<String, Iterable<String>>>sampleByCity(JavaPairRDD<String, Iterable<String>> data){
		/*This is something I was working on that should be close to what we'll need to sample by city.
        I never finished it though, because I didn't have the list of cities until sam supplied them.
        Here are the cities...
		 */

		String[] cities = {"AB","ON","QC","NV","AZ","PA","OH","WI","NC","IL","NYK"};
		JavaPairRDD<String, String> provinceClusters = data.mapToPair(s->{
			Iterator<String> iter1 = s._2().iterator();
			String province = iter1.next(); // state

			return new Tuple2<String, String>(province, s._1());
		});

		JavaPairRDD<String, Iterable<String>> d = provinceClusters.groupByKey();
//		data.foreach(s -> System.out.println(s._1() + ": " + Iterators.size(s._2().iterator())));
		provinceClusters = d.mapToPair(s -> {
			List<String> list = Lists.newArrayList(s._2());
			int num = ThreadLocalRandom.current().nextInt(0, Iterators.size(s._2().iterator()));
			return new Tuple2<String, String>(s._1(),list.get(num));
		});
		List<Tuple2<String, String>> cluster = new ArrayList<Tuple2<String, String>>();
		List<Tuple2<String, String>> tmp1 = provinceClusters.collect();
		for (Tuple2<String, String> t : tmp1) {
			for (String s : cities) {
				if (s.equalsIgnoreCase(t._1())) {
					cluster.add(t);
				}
			}
		}
		List<Tuple2<String, Iterable<String>>> output = new ArrayList<Tuple2<String, Iterable<String>>>();
		List<Tuple2<String, Iterable<String>>> tmp2 = data.collect();
		for (Tuple2<String, Iterable<String>> t : tmp2) {
			for (Tuple2<String, String> s : cluster) {
				if (s._2().equalsIgnoreCase(t._1())) {
					output.add(t);
				}
			}
		}
//		output.forEach(s -> System.out.println(s));
		return output;
	}
	
    public static List<Tuple2<String,Iterable<String>>> randomSample(JavaPairRDD<String, Iterable<String>> data, int k){
        return data.take(k);
    }

    /* The method, classify, will sort each business into the most similar cluster. To do this, I plan to compare
    each dimension (lat, lon, rating) and normalize the comparison.

    -90 <= lat <= 90
    -180 <= lon <= 180
    1 <= rating <= 5

    I shall normalize these values into a comparison between 0 & 1
    and I will add these 3 values together to get a result between 0 & 3
   */

    // This returns a JavaPairRDD such that...
    /*
            Business -> ClusterID,
                        State,
                        latitude,
                        longitude,
                        Yelp rating,
                        Number of reviews,
                        Categories


     */

    public static JavaPairRDD<String, Iterable<String>> classify(List<Tuple2<String, Iterable<String>>> classes, JavaPairRDD<String, Iterable<String>> data, boolean first){
        JavaPairRDD<String, Iterable<String>> clusters = data.mapToPair(s->{
            //double[] comps = new double[classes.size()];
            double min = 10;
            int targetCluster = 20;
            Iterator<String> iter1 = s._2().iterator();
            if(!first){
                String clst = iter1.next();
                System.out.println("================================================== INITIAL CLUSTER: " + clst + " ===================================================================");
                if(clst.length() > 3 || !clst.contains("[0-9]+")){
                    return s;
                }
                targetCluster = Integer.parseInt(clst);
            }


            String state = iter1.next();
            System.out.println("============================== STATE: " + state + "==============================================="); // state
            if(state.length() > 3 || !iter1.hasNext()){
                // This means that the data is corrupted and this is not actually a STATE, meaning the next iter.next()
                // might not exist. Try to gloss over this...
                return s;
            }
            String lat1 = iter1.next();
            String lon1 = iter1.next();
            String rating1 = iter1.next();
            System.out.println("======================================================== lat1: " + lat1 + "====================================================");
            System.out.println("======================================================== lon1: " + lon1 + "====================================================");
            System.out.println("======================================================== rating1: " + rating1 + "====================================================");

            // Skip past entries without lat and lon
            if(lat1.contains("None")){
                return s;
            }
            double lat1Norm = Double.parseDouble(lat1);
            double lon1Norm = Double.parseDouble(lon1);
            double rating1Norm = Double.parseDouble(rating1);
            int numReviews = Integer.parseInt(iter1.next());
            lat1Norm = (lat1Norm + 90)/180;
            lon1Norm = (lon1Norm + 180)/360;
            rating1Norm = rating1Norm * (Math.log(numReviews)/Math.log(7968))/5;
            double score1 = lat1Norm + lon1Norm + rating1Norm;
            for(int i = 0; i < classes.size(); i++){
                Tuple2<String, Iterable<String>> tmp = classes.get(i);

                Iterator<String> iter2 = tmp._2.iterator();
                if(first == true){
                    //iter2.next(); // clusterID (THERE IS NO CLUSTERID YET
                    iter2.next(); // state
                }

                String lat2 = iter2.next();
                String lon2 = iter2.next();
                String rating2 = iter2.next();
                double lat2Norm = Double.parseDouble(lat2);
                double lon2Norm = Double.parseDouble(lon2);
                double rating2Norm = Double.parseDouble(rating2);
                lat2Norm = (lat2Norm + 90)/180;
                lon2Norm = (lon2Norm + 180)/360;
                //number of stars*(log(reviews)/log(7968)) where 7968 is the max number of reviews
                rating2Norm = rating2Norm/5;
                double score2 = lat2Norm + lon2Norm + rating2Norm;

                double score = Math.abs(score1 - score2);
                //System.out.println("======================================================= SCORE: " + score + " ===========================================================");
                if(score < min){
                    min = score;
                    targetCluster = i;
                }else{

                }



            }

            // Write the new target cluster as the first element in the Iterable<String>.
            ArrayList<String> vals = new ArrayList<String>();
            System.out.println("=========================================================== Target Cluster: " + targetCluster + "==============================================================");
            vals.add("" + targetCluster);
            Iterator<String> iter = s._2.iterator();
            while(iter.hasNext()){
                vals.add(iter.next());
            }


           return new Tuple2<String, Iterable<String>>(s._1, vals);
        });
        return clusters;
    }

    /* getMeans returns a list of dummy businesses, whose data represents the mean lat, long, and
        rating for its cluster.

        The JavaPairRDD, clusters, contains the following information...

                Business -> ClusterID,
                        State,
                        latitude,
                        longitude,
                        Yelp rating,
                        Number of reviews,
                        Categories

        How will I take an average of lat and lon?

            for each lat_i:
                lat_i = lat_i + 90
                totalLat_i += lat_i
            avgLat_i = (totalLat_i/numi) - 90

            for each lon_i:
                lon_i = lon_i + 180
                totalLon_i += lon_i
            avgLon_i = (totalLon_i/numi) - 180
     */
    public static List<Tuple2<String, Iterable<String>>> getMeans(JavaPairRDD<String, Iterable<String>> clusters){
        // means will hold the following information for each cluster i=0-10
        /*
                Cluster_i ->    avgLat_i        = totalLat_i/numi
                                avgLon_i        = totalLon_i/numi
                                avgRating_i     = totalRating_i/numi
                                numi
         */
        ArrayList<Tuple2<String, Iterable<String>>> means = new ArrayList<>(11);
        for(int i = 0; i < 11; i++){
            ArrayList<String> vals = new ArrayList<>(4);
            vals.add(0, "0.0"); // totalLat_i
            vals.add(1, "0.0"); // totalLon_i
            vals.add(2,"0.0"); // totalRating_i
            vals.add(3,"0"); // numi
            Tuple2<String, Iterable<String>> item = new Tuple2<>("Cluster_" + i, vals);
            means.add(i, item);
        }

        // keep track of how many elements are in each cluster.


        clusters.mapToPair(s->{

            // grab data from this business
            Iterator<String> iter = s._2.iterator();
            int cluster = Integer.parseInt(iter.next());
            iter.next(); // state
            double lat = Double.parseDouble(iter.next());
            double lon = Double.parseDouble(iter.next());
            double rating = Double.parseDouble(iter.next());

            // grab the avg values
            Tuple2<String, Iterable<String>> avg = means.get(cluster);
            Iterator<String> iter2 = avg._2.iterator();
            double totalLat = Double.parseDouble(iter2.next());
            double totalLon = Double.parseDouble(iter2.next());
            double totalRating = Double.parseDouble(iter2.next());
            int count = Integer.parseInt(iter2.next());

            // update avg values
            lat += 90;
            lon += 180;
            totalLat += lat;
            totalLon += lon;
            totalRating += rating;
            count++;

            ArrayList<String> vals = new ArrayList<String>(4);
            vals.add(0,Double.toString(totalLat));
            vals.add(1,Double.toString(totalLon));
            vals.add(2,Double.toString(totalRating));
            vals.add(3,Integer.toString(count));

            // update new vals in means
            means.remove(cluster);
            means.add(cluster, new Tuple2<String, Iterable<String>>("Cluster" + cluster, vals));

            return s;
        });

        // calculate the means
        for(int i = 0; i < 11; i++){
            Tuple2<String, Iterable<String>> tmp = means.get(i);

            // Extract values
            Iterator<String> iter = tmp._2.iterator();
            double totalLat = Double.parseDouble(iter.next());
            double totalLon = Double.parseDouble(iter.next());
            double totalRating = Double.parseDouble(iter.next());
            int count = Integer.parseInt(iter.next());

            // calculate averages
            totalLat = (totalLat/count) - 90;
            totalLon = (totalLon/count) - 180;
            totalRating = totalRating/count;


            // store the averages
            ArrayList<String> avgs = new ArrayList<String>(4);
            avgs.add(0, Double.toString(totalLat));
            avgs.add(1, Double.toString(totalLon));
            avgs.add(2, Double.toString(totalRating));
            avgs.add(3, Integer.toString(count));
            Tuple2<String, Iterable<String>> result = new Tuple2<String, Iterable<String>>(tmp._1, avgs);
            means.remove(i);
            means.add(i, result);
        }

        return means;
    }
}

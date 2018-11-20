package YelpDataAnalysis;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Cluster3D {

    /* The format of data are as follows:

            Business Name ->    State,
                                latitude,
                                longitude,
                                Yelp rating,
                                Number of reviews,
                                Categories

      Required variables:

      int k = number of clusters
      int s = number of states ( this includes canadian provinces)
      int x = number of random businesses per state
      int totalSize = number of total businesses in the dataset = 142544

      Step1) Define an initial (random) solution as vectors of means.

            m(t=0) = [m1, m2, ..., mk]

            It would be wise to space these random vectors out according to state. To do this,
            I shall create a new map...

            State -> Tuple2< Business, Iterable<String>

            ... and do a reduce. This will result in...

            String -> List<Tuple2<String, Iterable<String>>>
              map       key

            ... or something like that. Then I for each state I will need to select
            x amount of random businesses from each state where...

            x = k * (totalSize/key.size())



    */

    public static void kMeansCluster(JavaPairRDD<String, Iterable<String>> data){
        System.out.println("============================================================================= Print Data ===================================================================================");

        // Step 1) random sampling
        //      Sample by state
        JavaPairRDD<String,Iterable<String>> stateMap = data.mapToPair(s -> {
            Iterator iter = s._2.iterator();
            String key = null;
            String lat = null;
            String lon = null;
            String rating = null;
            String num = null;
            String cat = null;
            int i = 0;

            while(iter.hasNext()){
                String next = iter.next().toString();
                if(i==0){
                    key = next;
                }else if(i == 1){
                    lat = next;
                }else if(i == 2){
                    lon = next;
                }else if(i == 3){
                    rating = next;
                }else if(i == 4){
                    num = next;
                }else if(i == 5){
                    cat = next;
                }
                i++;
            }

            String[] vals = new String[6];
            vals[0] = s._1;
            vals[1] = lat;
            vals[2] = lon;
            vals[3] = rating;
            vals[4] = num;
            vals[5] = cat;

            //Tuple2<String, List<String>> result = new Tuple2<String, Iterable<String>>(key, );
            return s;
        });



    }
}

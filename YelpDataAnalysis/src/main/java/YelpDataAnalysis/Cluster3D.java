package YelpDataAnalysis;

import org.apache.spark.api.java.JavaPairRDD;
import org.codehaus.janino.Java;
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



    */

    public static void kMeansCluster(JavaPairRDD<String, Iterable<String>> data){
        System.out.println("============================================================================= Print Data ===================================================================================");

        // Step 1)
        // random sampling
        List<Tuple2<String, Iterable<String>>> sample = randomSample(data, 11);



        // Step 2)

        /*
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
        */



    }

    public static List<Tuple2<String, Iterable<String>>>sampleByCity(JavaPairRDD<String, Iterable<String>> data){
        /*This is something I was working on that should be close to what we'll need to sample by city.
        I never finished it though, because I didn't have the list of cities until sam supplied them.
        Here are the cities...

        Calgary AB Canada
        Toronto ON Canada
        Montreal QC Canada
        Las Vegas NV US
        Phoenix AZ US
        Pittsburgh PA US
        Cleveland OH US
        Madison WI US
        Charlotte NC US
        Champaign IL US
        Scarborough NYK UK

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
        */
        return null;
    }
    public static List<Tuple2<String,Iterable<String>>> randomSample(JavaPairRDD<String, Iterable<String>> data, int k){
        return data.take(k);
    }

    // This method will sort each business into
    public JavaPairRDD<String, Iterable<String>> classify(List<Tuple2<String, Iterable<String>>> classes, JavaPairRDD<String, Iterable<String>> data){
        data.mapToPair(s->{
            for(int i = 0; i < classes.size(); i++){
                Tuple2<String, Iterable<String>> tmp = classes.get(i);
                Iterator<String> iter1 = s._2().iterator();
                iter1.next(); // state
                String lat1 = iter1.next();
                String lon1 = iter1.next();
                String rating1 = iter1.next();
                double r1 = Double.parseDouble(rating1);

            }
           return s;
        });
        return null;
    }
}

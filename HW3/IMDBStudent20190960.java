//movie.dat
import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.*;
import java.util.*;
public final class IMDBStudent20190960 {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: IMDBStudent20190960 <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("IMDBStudent20190960")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
             public Iterator<String> call(String s) {
                        ArrayList<String> result = new ArrayList<String>();
                        String [] splited = s.split("::");
                        StringTokenizer itr = new StringTokenizer(splited[2], "|");

                        while(itr.hasMoreTokens()) {
                                String genre = itr.nextToken();
                                result.add(genre);
                        }
                    return result.iterator();
                }
        };
        
        // mapper
        JavaRDD<String> words = lines.flatMap(fmf);
        PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call (String s) {
                    return new Tuple2(s, 1);
            }
        };

        JavaPairRDD<String, Integer> ones = words.mapToPair(pf);
        // reducer
        Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                    return x + y;
            }
        };

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2);
    
        JavaRDD<String> resultRdd = counts.map(x -> x._1 + " " + x._2);
        // write result
        resultRdd.saveAsTextFile(args[1]);
        spark.stop();
	}
}


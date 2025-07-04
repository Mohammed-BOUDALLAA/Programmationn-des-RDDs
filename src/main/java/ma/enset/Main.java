package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Main {
    public static void main(String[] args) {
        // Set JVM options for JDK 17 compatibility
        System.setProperty("spark.driver.extraJavaOptions",
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED " +
                        "--add-opens java.base/java.lang=ALL-UNNAMED");

        SparkConf conf = new SparkConf()
                .setAppName("Ventes Analysis")
                .setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // Load data - use resources directory for data files
            JavaRDD<String> rddLines = sc.textFile("C:\\Users\\newuser\\Downloads\\Compressed\\SparkVentesrdds\\src\\main\\data\\ventes.txt");

            // 1. Total sales count
            long totalVentes = rddLines.count();
            System.out.println("Total Ventes: " + totalVentes);

            // Process data
            JavaRDD<String[]> rddOrders = rddLines.map(line -> line.split(" "));

            // 2. Total sales by city
            JavaPairRDD<String, Integer> totalParVille = rddOrders
                    .mapToPair(order -> new Tuple2<>(order[1], Integer.parseInt(order[3])))
                    .reduceByKey(Integer::sum);

            System.out.println("\nTotal par ville:");
            totalParVille.collect().forEach(entry ->
                    System.out.println("Ville: " + entry._1 + " - Total: " + entry._2));

            // 3. Total sales by city and year
            JavaPairRDD<String, Integer> totalParAnneeVille = rddOrders
                    .mapToPair(order -> {
                        String[] dateParts = order[0].split("-"); // Assuming format YYYY-MM-DD
                        String annee = dateParts[0];
                        return new Tuple2<>(order[1] + "-" + annee, Integer.parseInt(order[3]));
                    })
                    .reduceByKey(Integer::sum)
                    .sortByKey();

            System.out.println("\nTotal par ville et année:");
            totalParAnneeVille.collect().forEach(entry -> {
                String[] parts = entry._1.split("-");
                System.out.println("Ville: " + parts[0] + " - Année: " + parts[1] +
                        " - Total: " + entry._2);
            });
        }
    }
}

import org.apache.spark._
import org.apache.log4j._

object WordCountAssignment {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext(new SparkConf().setAppName("Spark Word Count").setMaster("local"))

    // Read each line of my book into an RDD
    val input = sc.textFile("src/main/resources/sample.txt")
    //val input = sc.textFile("src/main/resources/news-2016-2017.txt")

    // Split into words separated by a space character; flatMap one to many transformation
    val words = input.flatMap(x => x.split(" "))

    // Count up the occurrences of each word
    val wordCounts = words.countByValue()

    //logic to sort the results.
    val sortedWordCounts = wordCounts.toSeq.sortBy(_._2).reverse

    //now take only 500 from the collection and print.
    sortedWordCounts.take(500).foreach(println)

    // Print the results.
    //wordCounts.foreach(println)

  }
}


import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scalaz._
import Scalaz._
import java.io._

object Main {

  //only works for V :< Integer
  def reduceByKey[K,V](collection: Traversable[Tuple2[K, V]])(implicit num: Numeric[V]) = {
    import num._
    collection
      .groupBy(_._1)
      .map { case (group: K, traversable) => traversable.reduce{(a,b) => (a._1, a._2 + b._2)} }
  }

  def count_words(str:String) =
    //reduceByKey( str.split(" ").map( w => (w, 1) ) )
    str.split(" ").map( w => Map(w -> 1)).reduce((a, b) => a |+| b)

  def map_action(line:String): (Long, List[Map[String, Int]]) = {
    val d = line.split("\t")
    return (d(0).toLong, d.toList.tail.map(e => count_words(e)))
  }

  def tuple_toString(tuple:(Long, List[Map[String, Int]])): String =
    tuple._1 +tuple._2.map( m => if (m==Nil || m==null || m==Map.empty)  ""
                                  else m.map( e => e._1 +" " +e._2 ).reduce( _ +"," +_ ) )
                        .foldLeft("")( _ +"\t" +_ )

  def getGlobalDict(countWordsRDD: RDD[(Long, List[Map[String, Int]])], idx: Int): Map[String, Int] =
    countWordsRDD.map(e => e._2).map(x => if (x.length > idx) x(idx) else Map.empty[String, Int]).reduce( (a, b)  => a |+| b )

  def writeFile(filename:String, content:String): Unit = {
    val pw = new PrintWriter(new File(filename))
    pw.write(content)
    pw.close()
  }

  def printSample(globalDict: Map[String, Int]): String = {
    if(globalDict!=null && globalDict.size >= 20)
      globalDict.take(20).mkString(",")
    else if(globalDict!=null)
      globalDict.mkString(",")
    else ""
  }

  def main(args:Array[String]) = {
    var cores = Runtime.getRuntime().availableProcessors()
    if(cores<1) {
      System.err.println("Can't detect the number of cores")
      System.exit(1)
    }

    val sc = new SparkContext("local[" +cores +"]",
      "activeLearning-tfidf",
      "/home/perez/ITAM/DEIIS/spark/spark-0.9.1-bin-hadoop1",
      List("target/scala-2.10/activelearning-tf_2.10-1.0.jar"))

    //init
    val tf = sc.textFile("/home/perez/ITAM/DEIIS/ActiveLearning/datos/twitter-users_en.csv.preprocess")

    //term freq vectors
    val countWords: RDD[(Long, List[Map[String, Int]])] = tf.map(line => map_action(line))

    val countWordsText = countWords.map(obj => tuple_toString(obj))

    //global dictionaries
    //zero based index: name(2), time_zone(8), description(11), location(12)
    //in the list: name(0), time_zone(1), description(2), location(3)
    val colnames = List("name", "timezone", "description", "location")
    val idxs = List(0,1,2,3)
    val globalDicts = idxs.map(i => getGlobalDict(countWords, i))

    //save
    println("#################################### Count: " +countWordsText.count())
    countWordsText.saveAsTextFile("../datos/twitter-users_en.csv.tf_vectors")
    idxs.foreach(i => writeFile("../datos/twitter-users_en.csv.globalDict_" +colnames(i) +".txt", globalDicts(i).keys.mkString("\n")))

    //print some results
    idxs.foreach(i => println("GlobalDict_" +colnames(i) +": " +printSample(globalDicts(i))))
    println("Term Freq. Vectors: ")
    countWords.take(10).map( a => tuple_toString(a) ).map(println(_))
    println("Continuing happily")
  }
}

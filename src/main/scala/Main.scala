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
    reduceByKey( str.split(" ").map( w => (w, 1) ) )

  def map_action(line:String): (Long, List[Map[String, Int]]) = {
    val d = line.split("\t")
    return (d(0).toLong, d.toList.tail.map(e => count_words(e)))
  }

  def tuple_toString(tuple:(Long, List[Map[String, Int]])): String =
    tuple._1 +tuple._2.map( m => if (m==Nil || m==null || m==Map.empty)  ""
                                  else m.map( e => e._1 +" " +e._2 ).reduce( _ +"," +_ ) )
                        .foldLeft("")( _ +"\t" +_ )

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
    val tf = sc.textFile("/home/perez/ITAM/DEIIS/ActiveLearning/datos/twitter-users_en.csv.preprocess/part-00002")

    //term freq vectors
    val countWords = tf.map(line => map_action(line))

    val countWordsText = countWords.map(obj => tuple_toString(obj))

    val globalDict = countWords.map(e => e._2).map(x => if (x.length > 0) x(0) else Map.empty[String, Int]).reduce( (a, b)  => a |+| b )

    val globalDictText = globalDict.map( a => a._1 ).foldLeft("")( _ + " " + _ )

    //global dictionary
    //val global1 = countWords.map(e => e._2(1)).reduce((a,b) => a |+| b)

    //save
    countWordsText.saveAsTextFile("count")
    val pw = new PrintWriter(new File("globalDict"))
    pw.write(globalDictText)
    pw.close()

    //print some results
    println("GlobalDict: " +(if(globalDictText!=null && globalDictText.length() >= 8) globalDictText.substring(0, 9) else "") )
    println("Term Freq. Vectors: ")
    countWords.take(10).map( a => tuple_toString(a) ).map(println(_))
    println("Continuing happily")
  }
}

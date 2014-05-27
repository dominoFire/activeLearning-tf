package mx.itam.deiis.activelearning.tf

import org.apache.spark.rdd.RDD
import scalaz._
import Scalaz._
import mx.itam.deiis.activelearning.Utils

class MainTF {}

object MainTF {

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

  def printSample(globalDict: Map[String, Int]): String = {
    if(globalDict!=null && globalDict.size >= 20)
      globalDict.take(20).mkString(",")
    else if(globalDict!=null)
      globalDict.mkString(",")
    else ""
  }

  def main(args:Array[String]) = {
    val prop = Utils.getConfigProperties(args)
    val sc = Utils.SparkContextFromConfig(prop, classOf[MainTF])

    //input
    val preprocessDatasetFile = prop.getProperty("preprocessDatasetFile")
    val tfGlobalDictFile = prop.getProperty("tfGlobalDictFile")
    //output
    val tfVectorsDatasetFile = prop.getProperty("tfVectorsDatasetFile")

    //init
    val tf = sc.textFile(preprocessDatasetFile)

    //term freq vectors
    val countWords: RDD[(Long, List[Map[String, Int]])] = tf.map(line => map_action(line))

    val countWordsText = countWords.map(obj => tuple_toString(obj))

    //global dictionaries
    //zero based index: name(2), time_zone(8), description(11), location(12)
    //in the list: name(0), time_zone(1), description(2), location(3)
    //val colnames = List("name", "timezone", "description", "location")
    val idxs = List(0,1,2,3)
    val globalDicts = idxs.map(i => getGlobalDict(countWords, i))

    val global = globalDicts.reduce( (a, b) => a |+| b)

    //save
    //println("#################################### Count: " +countWordsText.count())
    countWordsText.saveAsTextFile(tfVectorsDatasetFile)
    //idxs.foreach(i => writeFile("../datos/twitter-users_en.csv.globalDict_" +colnames(i) +".txt", globalDicts(i).keys.mkString("\n")))
    Utils.writeFile(tfGlobalDictFile, global.keys.mkString("\n"))
    //print some results
    //idxs.foreach(i => println("GlobalDict_" +colnames(i) +": " +printSample(globalDicts(i))))
    //println("Term Freq. Vectors: ")
    //countWords.take(10).map( a => tuple_toString(a) ).map(println(_))

    sc.stop()

    println("Continuing happily")
  }
}

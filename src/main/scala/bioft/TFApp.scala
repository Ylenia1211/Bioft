package bioft
import org.apache.spark.api.java.{JavaPairRDD, JavaSparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
object TFApp extends  App {


  def hello(): Unit = {
    print("CIAO")
  }

  def process(k: Int, filePath: String, name_file:String, sc:JavaSparkContext, sqlContext: SQLContext): DataFrame = {
    println("Hello, Wolrd")

    val util = utils()
    val K: Int = k + 1
    val broadcastK: Broadcast[Int] =  sc.broadcast(K)
    val sequence_file = sc.textFile(filePath + name_file, 1)

    var filterRDD_old = sequence_file
      .map {line =>
        if (line.startsWith(">")) ("*")
        else line
      }
      //.filter(x => !x.startsWith(">"))
      .map(x => x.toLowerCase())
      //.map(x => x.replaceAll("[^(a|g|c|t)]", ""))
      .filter(x => x.nonEmpty)


    print(filterRDD_old.collect())
    var num_line = filterRDD_old.map(_.split("\n")).map(line => (line,1)).map(_._2).reduce(_ + _)
    print("\nNumero righe: ",  num_line)

    var kmerSeq = util.mapper_kmers_simple(k, filterRDD_old, broadcastK)
    var kmersCount: RDD[(String, Integer)] = kmerSeq.rdd.reduceByKey(_ + _)
    var kmers_doc: Integer = kmersCount.map(_._2).reduce(_ + _)
    println("size doc (simple count)  = " + kmers_doc)

    val lookupRDD: RDD[(String, Int)] = util.kmers_between(filterRDD_old, num_line,sc)
    //lookupRDD.take(10).foreach(f => print(f + "\n"))

    val result_subSeq: RDD[String] = lookupRDD.map(f => f._1.slice(Math.abs((f._1.length) - f._2) - (K - 1)+ 1,  Math.abs((f._1.length) - f._2) + (K - 1) - 1).mkString(""))
    result_subSeq.foreach(f => print(f + "\n"))

    var kmerSubSeq: JavaPairRDD[String, Integer] = util.mapper_kmers_simple(k, result_subSeq, broadcastK)
    var kmersSub: RDD[(String, Integer)] = kmerSubSeq.rdd.reduceByKey(_ + _) // Reduce to count Frequency Kmers in Doc => Sequence

    var kmers_doc_substring: Integer =  kmersSub.map(_._2).reduce(_ + _)//nuovo kmers_doc
    println("size doc (substring between rows count)= " + kmers_doc_substring)

    /**Unisce i Map Reduce dei kmers_simple con i kmers_between */
    var kmerTot: RDD[(String, Integer)] =  util.create_emptyRDD(filterRDD_old)
    kmerTot = kmerTot.union(kmersSub)
    val kmersTot: RDD[(String, Integer)] = kmersCount.union(kmerTot)
    var kmersTotalCount: RDD[(String, Integer)]  = kmersTot.reduceByKey(_ + _)
    var kmers_doc_complete: Integer =  kmersTotalCount.map(_._2).reduce(_ + _)//nuovo kmers_doc
    println("Final size doc (total) = " + kmers_doc_complete)

    val df_kmersTotalCount =  sqlContext.createDataFrame(kmersTotalCount)

    //val filteredDataFrame = df_kmersTotalCount.filter(!col("_1").contains("*"))





    return df_kmersTotalCount


  }
}

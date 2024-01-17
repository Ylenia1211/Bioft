package bioft

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.function.PairFlatMapFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import java.util
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{expr, lit, sum}

import java.io.File

  case class utils() {

    def mapper_kmers_simple(start: Int, filterRDD: RDD[String], broadcastK: Broadcast[Int]): JavaPairRDD[String, Integer] = {
      var kmerSeq: JavaPairRDD[String, Integer] = filterRDD.toJavaRDD().flatMapToPair(new PairFlatMapFunction[String, String, Integer] { //x => countKmers(x,k_max)
        override def call(sequence: String): util.Iterator[(String, Integer)] = {
          //print(sequence+"\n")
          var listkmers = new util.ArrayList[(String, Integer)]
          val k_max = broadcastK.value
          for (j <- start to k_max - 1) { //j <- 2
            var i = 0
            while ( {
              i < sequence.length - j + 1
            }) {
              val kmer = sequence.substring(i, j + i)
              listkmers.add(new (String, Integer)(kmer, 1))
              i += 1
            }
          }
          return listkmers.iterator()
        }
      })
      //var kmersCount: RDD[(String, Integer)] = kmerSeq.rdd.reduceByKey(_ + _)
      return kmerSeq
    }

    def create_emptyRDD(rddTemp: RDD[String]): RDD[(String, Integer)] = {
      var kmersCumulative: RDD[(String, Integer)] = rddTemp.toJavaRDD().flatMapToPair(new PairFlatMapFunction[String, String, Integer] {
        override def call(t: String): util.Iterator[(String, Integer)] = {
          val emptyList = new util.ArrayList[(String, Integer)]
          return emptyList.iterator()
        }
      })
      return kmersCumulative
    }

    def kmers_between(filterRDD: RDD[String], size: Int, sc: SparkContext): RDD[(String, Int)] = {

      //val junkFirst = filterRDD.first()
      //junkFirst.foreach(println)
      val fdata = filterRDD //.filter(x => x != junkFirst) // removes the first line
      val total = fdata.count().toInt //size rdd
      //print("total", total)

      var withoutHeaderFooter = sc.parallelize(Seq(""))
      withoutHeaderFooter = fdata

      val findsub2 = withoutHeaderFooter.zipWithIndex
        .map({ case (line, index) => (index + 1 % 2, (index + 1, line)) })


      var findsub2_pair = findsub2
        .filter({ case (indList, (index, line)) => indList % 2 == 0 })
        .map({ case (indexPair, (index, line)) => (indexPair - 1, (index, line)) })

      var findsub2_dispair = findsub2
        .filter({ case (indList, (index, line)) => indList % 2 != 0 })
        .map({ case (indexPair, (index, line)) => (indexPair - 1, (index, line)) })


      //print("indexing originale: \n")
      //findsub2.foreach(f => print(f + "\n"))

      //print("indexing pari: \n") //da aggregare alle chiavi dispari nel findsub index originale
      //findsub2_pair.foreach(f => print(f + "\n"))
      //print("\n\n\n----")

      //print("indexing dispari: \n") //da aggregare alle chiavi pari nel findsub index originale
      //findsub2_dispair.foreach(f => print(f + "\n"))
      //print("\n\n\n----")

      val findsub2_pair_aggr = findsub2_pair.union(findsub2
        .filter({ case (indList, (index, line)) => indList % 2 != 0 }))
        .aggregateByKey(List.empty[(Long, String)])(
          { case (aggrList, (index, line)) => (index, line) :: aggrList },
          { case (aggrList1, aggrList2) => aggrList1 ++ aggrList2 }
        ).map({ case (key, aggrList) =>
        aggrList
          .sortBy({ case (index, line) => index })
          .map({ case (index, line) => line })
      }).filter(x => x.length > 1)
        .map({ case list => (list.mkString(""), list(1).length) })


      //findsub2_pair_aggr.foreach(f => print(f + "\n"))

      val findsub2_dispair_aggr = findsub2_dispair.union(findsub2
        .filter({ case (indList, (index, line)) => indList % 2 == 0 }))
        .aggregateByKey(List.empty[(Long, String)])(
          { case (aggrList, (index, line)) => (index, line) :: aggrList },
          { case (aggrList1, aggrList2) => aggrList1 ++ aggrList2 }
        ).map({ case (key, aggrList) =>
        aggrList
          .sortBy({ case (index, line) => index })
          .map({ case (index, line) => line })
      }).filter(x => x.length > 1)
        .map({ case list => (list.mkString(""), list(1).length) })

      //findsub2_dispair_aggr.foreach(f => print(f + "\n"))

      val lookupRDD = findsub2_pair_aggr.union(findsub2_dispair_aggr)
      //print("--- All Substring between rows\n")
      //lookupRDD.foreach(f => print(f + "\n"))

      return lookupRDD
    }


    def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
      dir.listFiles.filter(_.isFile).toList.filter { file =>
        extensions.exists(file.getName.endsWith(_))
      }
    }

    def getListOfFolder(dir: File, extensions: List[String]): List[File] = {
      dir.listFiles.filter(_.isDirectory).toList.filter { file =>
        extensions.exists(file.getName.endsWith(_))
      }
    }

    def createSave_TF_gene(pathSaveTf: String, kmersTotalCumulative: RDD[(String, Integer)], kmers_docCumulative: Int, name_sets: String, sparkSession: org.apache.spark.sql.SparkSession){

      var dfKmersSeq = sparkSession.createDataFrame(kmersTotalCumulative).toDF("kmers", "count")
      val requiredDf = dfKmersSeq.withColumn("id_file", lit(name_sets))
      val completeKmersDF = requiredDf.withColumn("kmers_doc", lit(kmers_docCumulative))
      val Tf_Df = completeKmersDF.withColumn("TF", expr("count/kmers_doc"))
      Tf_Df.write.mode("overwrite").parquet(pathSaveTf + name_sets)
      println("File parquet:" + name_sets + "saved!")
      //Tf_Df.show()
    }


    def compute_tf_single_seq(pathSaveTf: String, start: Int, filePath: String, name_file: String,  spark: org.apache.spark.sql.SparkSession){
      val K: Int = start + 1
      val broadcastK: Broadcast[Int] =  spark.sparkContext.broadcast(K)
      val sc = spark.sparkContext
      val sequence_file = sc.textFile(filePath + name_file, 1)

      var filterRDD_old = sequence_file
        .filter(x => !x.startsWith(">"))
        .map(x => x.toLowerCase())
        //.map(x => x.replaceAll("[^(a|g|c|t)]", ""))
        .filter(x => x.nonEmpty)

      var num_line = filterRDD_old.map(_.split("\n")).map(line => (line,1)).map(_._2).reduce(_ + _)
      //print("\nNumero righe: ",  num_line)

      //print(filterRDD_old.collect().foreach(println))
      var kmerSeq = mapper_kmers_simple(start, filterRDD_old, broadcastK)
      var kmersCount: RDD[(String, Integer)] = kmerSeq.rdd.reduceByKey(_ + _)
      var kmers_doc: Integer = kmersCount.map(_._2).reduce(_ + _)

      //println("Size doc Prima = " + kmers_doc)
      //var kmerSubSequence: RDD[(String, Integer)] = util.create_emptyRDD(filterRDD)

      val lookupRDD= kmers_between(filterRDD_old, num_line,sc)

      var result_subSeq: RDD[String] = lookupRDD.map(f => f._1.slice(((f._1.length) - f._2 - (K - 1)) + 1, ((f._1.length) - f._2 + (K - 1)) - 1).mkString(""))

      //result_subSeq.foreach(f => print(f + "\n"))

      var kmerSubSeq: JavaPairRDD[String, Integer] = mapper_kmers_simple(start, result_subSeq, broadcastK)

      var kmersSub: RDD[(String, Integer)] = kmerSubSeq.rdd.reduceByKey(_ + _) // Reduce to count Frequency Kmers in Doc => Sequence
      //kmerSubSeq.rdd.foreach(f => print(f + "\n"))

      var kmers_doc_substring: Integer =  kmersSub.map(_._2).reduce(_ + _)//nuovo kmers_doc
      //println("Size doc substring= " + kmers_doc_substring)

      /**Unisce i Map Reduce dei kmers_simple con i kmers_between */
      var kmerTot: RDD[(String, Integer)] =  create_emptyRDD(filterRDD_old)
      kmerTot = kmerTot.union(kmersSub)
      val kmersTot: RDD[(String, Integer)] = kmersCount.union(kmerTot)
      var kmersTotalCount: RDD[(String, Integer)]  = kmersTot.reduceByKey(_ + _)
      var kmers_doc_complete: Integer =  kmersTotalCount.map(_._2).reduce(_ + _)//nuovo kmers_doc
      //println("Size doc = " + kmers_doc_complete)
      createSave_TF_gene(pathSaveTf, kmersTotalCount, kmers_doc_complete, name_file.split("\\.").toList.head, spark)
    }
  }
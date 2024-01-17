package bioft
import org.apache.spark.SparkConf
import org.apache.spark
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, expr, lit, log, log10, size}

import java.io.File

object TFIDFApp {
  def compute_tfidf(k: Int, pathSavedTf: String, pathSaveTfIDF: String,  sc:JavaSparkContext, sqlContext: SQLContext, spark: SparkSession): DataFrame = {

    val util = utils()
    val directory_tfidf =new File(pathSaveTfIDF)

    if (!directory_tfidf.exists) {
      directory_tfidf.mkdir
      // If you require it to make the entire directory path including parents,
      // use directory.mkdirs(); here instead.
    }

    val lst_name_files=  util.getListOfFolder(new File(pathSavedTf), List(""))
    var name_files= sc.parallelize(lst_name_files).map(x => x.getAbsolutePath.split("/").takeRight(1).toList).collect().toList
    //print(name_files(0)(0))
    //print(name_files.length)
    //if(folder_comp == "single") {
    var first_parquet_file: org.apache.spark.sql.DataFrame = spark.read.load(pathSavedTf + name_files(0).head + "/*")
    //print(name_files)
    //print(name_files(0).head.split("_").toList)
    var N_size = 0


    for (ind <- 1 until (name_files.length)) {
      print(name_files(ind).head + "\n")
      var tempFile: org.apache.spark.sql.DataFrame = spark.read.load(pathSavedTf + name_files(ind).head + "/*")
      first_parquet_file = first_parquet_file.union(tempFile)
      N_size = first_parquet_file.select("id_file").distinct().count().toInt
    }
    /*
  }else{
    //solo con SET
    var first_parquet_file: org.apache.spark.sql.DataFrame =  spark.read.load(path = pathSavedTf +"*")
    N_size =  first_parquet_file.select("id_set").distinct().count().toInt
  }*/
    //print(first_parquet_file.select("id_file").distinct().count())
    var list_kmer = first_parquet_file.select("kmers").rdd.map(r => r(0))
    var kmer_shared: RDD[(String, Int)] = list_kmer
      .map(kmer => (kmer.toString,1))
      .reduceByKey(_+_)

    /** Calcolo DF ---> ho bisogno di (kmer, #di volte presenza tra i file)  */
    import sqlContext.implicits._
    val dfKmersDF = spark
      .createDataFrame(kmer_shared)
      .toDF("kmers", "countShared")
      .withColumn("N",  lit(N_size)) //lit(name_files.size))

    var idfDF = dfKmersDF.withColumn("IDF", log(expr("(N)/(countShared)"))) //idf base
    //.withColumn("IDF", log(expr("(1+N)/(1+countShared)")) + 1) //IDF inverse document frequency smooth

    /**
    +----------+-----+-------+---------+--------------------+
    |     kmers|count|id_SET|kmers_doc|                  TF|
    +----------+-----+-------+---------+--------------------+

                         JOIN
    +----------+-----------+---+-------------------+
    |     kmers|countShared|  N|                IDF| =
    +----------+-----------+---+-------------------+


     Final Dataframe
      +----------+-----+-------+---------+
    |     ID_SET|   KMERS    | TF-IDF|
    +----------+-----+-------+---------+

     **/

    val tf_idf = first_parquet_file
      .join(idfDF, "kmers")
      .withColumn("TF-IDF", expr("TF*IDF"))
      .sort($"TF-IDF".desc)

    tf_idf
      .write
      .mode("overwrite")
      .format("parquet")
      .save(pathSaveTfIDF + "tf_idf") //all

    return  tf_idf
   }
}

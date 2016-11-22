package se.kth.spark.hw2.test

import org.apache.spark._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{ Row, SQLContext, DataFrame }
import org.apache.spark.ml.PipelineModel

import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("homework2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._

    val support = 2
    
    val filePath = "src/main/resources/test.dat"
//    val filePath = "src/main/resources/T10I4D100K.dat"
    val transactions = sc.textFile(filePath)

    val numOfTransactions = transactions.count
    
    // Pass 1
    val items = transactions.flatMap(txn => txn.split(" "))
    val itemsInt = items.map(_.toInt)
//    itemsInt.foreach(println)
    val itemOnes = itemsInt.map(item => (item, 1))
    val itemCounts_C1 = itemOnes.reduceByKey(_+_)
//    print(itemCounts_C1.count())
//    itemCounts_C1.foreach(println)
    val itemCounts_L1 = itemCounts_C1.filter{case (item,sup) => sup >= support}
//    itemCounts_L1.foreach(println)  
//    print(itemCounts_L1.count())
    
        print("\n")
    print("\n")
    print("\n")
    
    // Pass 2
//    transactions.foreach(println)
//    itemCounts_L1.keys.foreach(println)
    
    val freqItemCombos = itemCounts_L1.keys.cartesian(itemCounts_L1.keys).filter{ case (a,b) => a < b }
//    val freqItemCombos = itemCounts_L1.keys.cartesian(itemCounts_L1.keys)
//    freqItemCombos.cache
//    freqItemCombos.collect().foreach(println)    
    val freqItemCombosSet = freqItemCombos.collect.toSet
    
    val itemsPerTxn = transactions.map(txn => txn.split(" "))
    val itemsPerTxnInt = itemsPerTxn.map(txnItems => txnItems.map(_.toInt))
    val txnCombos = itemsPerTxnInt.map(txnItems => txnItems.combinations(3).toArray ) //http://stackoverflow.com/questions/26493597/apache-spark-generate-list-of-pairs
//    val txnCombos2 = txnCombos.map(arr=>arr)
//    print(txnCombos2.count)
    
    val txnCombosStr = txnCombos.map(arrOfarr => arrOfarr.map(arrOfInt => arrOfInt.toSet.mkString(",")))
    
    txnCombosStr.foreach(_.foreach((println)))
    
//    val intersectionPairs = txnCombos.map(arrayOfPairs => arrayOfPairs.filter(pair => freqItemCombosSet.contains(pair)))
////    intersectionPairs.map(row => row.length).foreach(println)
//    val intersectionPairsOnes = intersectionPairs.flatMap(arrayOfPairs => arrayOfPairs.map(array => (array,1)))
//    val C2 = intersectionPairsOnes.reduceByKey(_+_)
////    C2.foreach(println)
////    print(C2.count())
//    val L2 = C2.filter{case ((a,b),sup) => sup >= support}
////    L2.foreach(println)
//    
//    print("\n")
//    print("\n")
//    print("\n")
//    val test = L2.keys.map{ case (a,b) => s"$a,$b" }
////    .cartesian(itemCounts_L1.keys)
//    test.foreach(println)
    
  }
}
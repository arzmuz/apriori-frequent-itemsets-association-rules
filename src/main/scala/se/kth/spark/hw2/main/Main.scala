package se.kth.spark.hw2.main

import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.util.Sorting.quickSort

object Main {
  
  val conf = new SparkConf().setAppName("lab1").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._
  import sqlContext._
  
  // Main function - may change variables here
  def main(args: Array[String]) 
  {
    val support = 1000
    val confidence = 0.5
    
//    val filePath = "src/main/resources/test.dat"
    val filePath = "src/main/resources/T10I4D100K.dat"
    
    apriori(filePath, support, confidence) 
  }

  def apriori(filePath: String, support: Int, confidence: Double) = 
  {  
    val transactions = sc.textFile(filePath)
    val numOfTransactions: Double = transactions.count

    // First run pass k=1 separately
    val L_1: org.apache.spark.rdd.RDD[(String, Int)] = aprioriPass1(transactions, support)
    val freqItems = getL1FreqItems(L_1)
    
    var L_kMinusOne: org.apache.spark.rdd.RDD[(String, Int)] = L_1
    
    var k: Int = 2
    
    // Then run passes k=2,.. until no more frequent itemsets can be generated.
    while(!L_kMinusOne.isEmpty())
    {
      val freqCombos: org.apache.spark.rdd.RDD[String] = generateCandidatesSet(L_kMinusOne, freqItems)
      val L_k: org.apache.spark.rdd.RDD[(String, Int)] = generateL_k(transactions, freqCombos, support, k)
    
      if(!L_k.isEmpty())
      {
        // Generate association rules
        genAssociationRules(confidence, numOfTransactions, L_1, L_k, L_kMinusOne)  
      }
      
      L_kMinusOne = L_k
      
      k += 1
    }
  }
  
  // Generate association rules
  def genAssociationRules(confidence: Double, numOfTransactions: Double, L_1: org.apache.spark.rdd.RDD[(String, Int)], 
      L_k: org.apache.spark.rdd.RDD[(String, Int)], L_kMinusOne: org.apache.spark.rdd.RDD[(String, Int)]) = 
  {
      val L_1Set = L_1.collect.toSet
      val L_kSet = L_k.collect.toSet
      val L_kMinusOneSet = L_kMinusOne.collect.toSet
    
      val itemsetsStr = L_k.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }
      val itemsets = itemsetsStr.map(str => str.split(",").toSet)
      
      val desiredSubsetSize = itemsets.take(1)(0).size
      val subsets = itemsets.map(s => (s, s.subsets.toSet.filter(ss => ss.size != 0 && ss.size != desiredSubsetSize)))

      val rules = subsets.map{ case (parentSet, childSet) => 
          childSet.map(setOfStr => 
            (getSupportFromL_k(L_kSet, ("[" + (parentSet.mkString(",")) + "]")),
                setOfStr, parentSet diff setOfStr)) 
        }
      val rulesFlattened = rules.flatMap(s => s)
//      val rulesFlattened = rules.flatMap{ case (p,c) => c }

      val rulesWithConfidence = rulesFlattened.map{ case (num, i, j) => (num, "[" + (i.mkString(",")) + "]", "[" + (j.mkString(",")) + "]") }
      .map{ case (num, str, j) => (str + "-->" + j + " with confidence: ",   
        ((num.toDouble / (if (str.replaceAll("[\\[()\\]]", "").split(",").size == 1) 
                    getSupportFromL_k(L_1Set, str)
                  else  
                    getSupportFromL_k(L_kMinusOneSet, str)).toDouble)
        
                    -
                    
                    ((if (j.replaceAll("[\\[()\\]]", "").split(",").size == 1) 
                    getSupportFromL_k(L_1Set, j)
                  else  
                    getSupportFromL_k(L_kMinusOneSet, j)).toDouble)/numOfTransactions)
        )
      }
      
      val filteredRulesWithConfidence = rulesWithConfidence.filter{ case (str, conf) => conf >= confidence }
        .map{ case (str, conf) => str + conf }
      
//      subsets.foreach((println))
//      rules.foreach((println))
//      rulesFlattened.foreach((println))
      println("\nAssociation rules:")  
      filteredRulesWithConfidence.foreach((println))
  }
  
  // Fetch support value for a given tuple
  def getSupportFromL_k(L_kSet: Set[(String, Int)], keyToFind: String): String =
  {
    val support = L_kSet.filter{ case (s,i) => s == keyToFind }.map{ case (s,i) => i }
    val supportStr = support.mkString("")
    
    return supportStr
  }
  
  // Run Apriori algorithm for pass 1
  def aprioriPass1(transactions: org.apache.spark.rdd.RDD[String], support: Int): org.apache.spark.rdd.RDD[(String, Int)] =
  {
    println("k = 1")
    
    // Pass 1
      val items = transactions.flatMap(txn => txn.split(" "))
      val itemOnes = items.map(item => ("["+item+"]", 1))
      val itemCounts_C1 = itemOnes.reduceByKey(_+_)
      val itemCounts_L1 = itemCounts_C1.filter{case (item,sup) => sup >= support}
      
    println("\nFrequent items:")
      itemCounts_L1.foreach(println)  
//      print(itemCounts_L1.count())
      
      return itemCounts_L1
  }
  
  // Only return the frequent items (without counts) for pass 1
  def getL1FreqItems(L1: org.apache.spark.rdd.RDD[(String, Int)]): org.apache.spark.rdd.RDD[String]  = 
  {
    val freqItems = L1.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }
    return freqItems
  }
  
  // General function to generate candidate sets
  def generateCandidatesSet(L_kMinusOne: org.apache.spark.rdd.RDD[(String, Int)], freqItems_L1: org.apache.spark.rdd.RDD[String])
  :org.apache.spark.rdd.RDD[String] = 
  {
    val tupleLength = L_kMinusOne.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }.top(1)
    val desiredTupleLength = tupleLength(0).split(",").size + 1
    
    val itemsets = L_kMinusOne.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }
      val freqItemCombos = itemsets.cartesian(freqItems_L1).map(x=>x.toString.replaceAll("[\\[()\\]]", "").split(",").toSet.toSeq.sorted)
      .filter(tupleSet => tupleSet.size == desiredTupleLength)
      .map(strArr => strArr.map(_.toInt)).collect.toSet
//      freqItemCombos.foreach((println)) 
      
      val freqItemCombosStr = freqItemCombos.map(setOfInts => setOfInts.mkString(","))
      
      val freqItemCombosRdd: org.apache.spark.rdd.RDD[String] = 
        sc.parallelize(freqItemCombosStr.toSeq)
        
    print("\n")
        
      return freqItemCombosRdd
  }
  
  // Generate L_k and report results
  def generateL_k(transactions: org.apache.spark.rdd.RDD[String], freqCombos: org.apache.spark.rdd.RDD[String], support: Int, k: Int)
  : org.apache.spark.rdd.RDD[(String, Int)] 
  = 
  {
    val itemsPerTxn = transactions.map(txn => txn.split(" "))
    val itemsPerTxnInt = itemsPerTxn.map(txnItems => txnItems.map(_.toInt))
    val txnCombos = itemsPerTxnInt.map(txnItems => txnItems.combinations(k).toArray)
//    .map(arr => arr.map(each => each.toString().replaceAll("[\\[()\\]]", "")))
//      .map(x=>x.toString.replaceAll("[\\[()\\]]", "").split(",").toSet)
//      .map(strArr => strArr.map(_.toInt)).collect.toSet
      
    val txnCombosStr = txnCombos.map(arrOfarr => arrOfarr.map(arrOfInt => arrOfInt.toSet.mkString(",")))
   
    val freqCombosSets = freqCombos.collect().toSet
      
    val intersection = txnCombosStr.map(arr => arr.filter(tuple => freqCombosSets.contains(tuple)))
    val intersectionOnes = intersection.flatMap(arrayOfPairs => arrayOfPairs.map(array => ("["+array+"]",1)))
    
//    val intersectionRdd: org.apache.spark.rdd.RDD[String] = sc.parallelize(intersection.toSeq.map(setOfInt => setOfInt.toString()))
//    val intersectionPairsOnes = intersectionRdd.flatMap(str => str.replaceAll("[\\[()\\]]", "").map(str => (str.toString(),1)))
    val C_k = intersectionOnes.reduceByKey(_+_)

    val L_k = C_k.filter{case (str,sup) => sup >= support}

    println("--- --- --- --- ---\n")
    println("k = " + k)
    if(L_k.isEmpty())
    {
      print("No more frequent itemsets.")  
    }
    else
    {
      println("\nFrequent itemsets:")
      L_k.foreach(println) 
    }
    
    return L_k
  }
}
package com.welab.BDL.UserInterests

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

/**
 * @author LJY
 */
object InformationGain {

  def informationEntropy(target_attribute: Array[Double]): Double = {
    var temp = scala.collection.mutable.HashMap[Double, Int]()
    for (item <- target_attribute) {
      if (temp.contains(item)) {
        temp(item) = temp(item) + 1
      } else {
        temp.put(item, 1)
      }
    }
    var Entropy = 0.0
    for (item <- temp) {
      Entropy += (-1) * (item._2.toDouble / target_attribute.length) * log2(item._2.toDouble / target_attribute.length)
    }

    Entropy
  }

  def log2(x: Double): Double = scala.math.log(x) / scala.math.log(2)
  
  def informationGain(sc: SparkContext, feature_attribute: RDD[(Double, Double)]): Double = {
    val target = feature_attribute.map { x => x._2 }.toArray()
    val Entropy1 = informationEntropy(target)

    val all_Entropy = sc.accumulator(0.0)
    feature_attribute.groupBy(x => x._1).foreach { x => all_Entropy += (x._2.size.toDouble / target.length) * informationEntropy(x._2.map(x => x._2).toArray)
    }
    println(Entropy1)
    println(all_Entropy.value)
    (Entropy1 - all_Entropy.value)
  }

  def main(args: Array[String]): Unit = {
    //    Vectors.dense()
    val conf = new SparkConf().setAppName("OneLevelClassification").setMaster("local")
    val sc = new SparkContext(conf)

    val data=sc.textFile("/user/hive/warehouse/bairong_summary")
    
    val result=data.foreach{x=>val fields=x.split("\t")
      println(fields.length)  
    }

  }
}
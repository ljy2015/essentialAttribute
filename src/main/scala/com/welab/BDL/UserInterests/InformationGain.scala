package com.welab.BDL.UserInterests

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.Matrix

/**
 * @author LJY
 */
object InformationGain {

  //计算某个属性的信息熵
  def inforEntropy(target_attribute: Array[Double]): Double = {
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

  //计算特征与目标特征之间的信息增益
  def inforGain(sc: SparkContext, feature_attribute: RDD[(Double, Double)]): (Double,Double) = {
    val target = feature_attribute.map { x => x._2 }.toArray()
    val Entropy1 = inforEntropy(target)

    val all_Entropy = sc.accumulator(0.0)
    feature_attribute.groupBy(x => x._1).foreach { x => all_Entropy += (x._2.size.toDouble / target.length) * inforEntropy(x._2.map(x => x._2).toArray)
    }

    val X = feature_attribute.map { x => x._1 }
    val Y = feature_attribute.map { x => x._2 }

    val correlation: Double = Statistics.corr(X, Y, "pearson")
/*    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
    // If a method is not specified, Pearson's method will be used by default. 
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")*/
    
    //    println(Entropy1)
    //    println(all_Entropy.value)
    ((Entropy1 - all_Entropy.value),correlation)
  }

  def main(args: Array[String]): Unit = {
    //    Vectors.dense()
    val conf = new SparkConf().setAppName("OneLevelClassification").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/user/hive/warehouse/bairong_summary")

    val result = data.take(10).foreach { x =>
      val fields = x.split("\t")
      fields.foreach { x => print(x + " ") }
    }



  }
}
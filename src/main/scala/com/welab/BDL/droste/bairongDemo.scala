package com.welab.BDL.droste

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import com.welab.BDL.UserInterests.InformationGain
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.classification.SVMWithSGD

/**
 * @author LJY
 */
object bairongDemo {
  //计算各属性的信息增益率
  def computeGainRate(sc: SparkContext, data: RDD[LabeledPoint]): (ArrayBuffer[Double], ArrayBuffer[Double]) = {

    val feature_length = data.first().features.size

    var gain_rate_scores = scala.collection.mutable.ArrayBuffer[Double]()
    var corr_scores = scala.collection.mutable.ArrayBuffer[Double]()

    for (i <- 0 to feature_length - 1) {
      val feature = data.map { x => x.features(i) }.collect()
      val feature_target = data.map { x => (x.features(i), x.label) }
      gain_rate_scores.append(InformationGain.inforGain(sc, feature_target)._1 / InformationGain.inforEntropy(feature))
      corr_scores.append(InformationGain.inforGain(sc, feature_target)._2)
    }
    gain_rate_scores.foreach { x => println("information gain rate is: " + x) }
    corr_scores.foreach { x => println("corr is: " + x) }
    (gain_rate_scores, corr_scores)
  }

  def select_feature(sc: SparkContext, filename: String, thr: Double): RDD[LabeledPoint] = {
    val data = sc.textFile(filename)
    val alldata = data.map { x =>
      val fields = x.split(":")
      val label = fields(1)
      val feature = fields(0).split(",").slice(1, fields(0).split(",").length).map { x => x.toDouble }
      if (label.toDouble > 30) {
        LabeledPoint(1, Vectors.dense(feature))
      } else {
        LabeledPoint(0, Vectors.dense(feature))
      }
    }
    val pos_occur = alldata.filter{x=>x.label==1}.count()
    val neg_occur = alldata.filter{x=>x.label==0}.count()

    //计算信息增益和相关系数
    val inforGain_corr = computeGainRate(sc, alldata)
    inforGain_corr._1.foreach { x => println("information gain rate is: " + x) }
    inforGain_corr._2.foreach { x => println("corr whit target variable is: " + x) }

    val pos_sample = alldata.filter { x => x.label == 0 }.sample(false, pos_occur.toDouble / neg_occur, 1)

    println("pos sample size is: " + pos_occur + "\n" + "neg sample size is: " + neg_occur)

    val ALL = pos_sample.union(alldata.filter { x => x.label == 1 })

    val gain_rate = sc.broadcast(computeGainRate(sc, ALL)._1)

    val result = ALL.map { x =>
      var select = scala.collection.mutable.ArrayBuffer[Double]()
      for (index <- 0 to gain_rate.value.length - 1) {
        if (gain_rate.value(index) > thr) {
          select.append(x.features(index))
        }
      }
      
      LabeledPoint(x.label, Vectors.dense(select.toArray))
    }
    result
  }

  def my_lr_model(sc: SparkContext, filename: String, cv: Int, thr: Double, model_weights_path: String): Double = {

    val ALL = select_feature(sc, filename, thr)
    var modelweights = scala.collection.mutable.ArrayBuffer[Double]()
    var maxAUC = 0.0

    var kflod = ArrayBuffer[Double]()
    var scores = ArrayBuffer[Double]()
    for (i <- 1 to cv) {
      kflod.append(1.0 / cv)
    }

    // Split data into training (60%) and test (40%).
    val splits = ALL.randomSplit(kflod.toArray, seed = 11L)

    for (i <- 0 to cv - 1) {
      val test = splits(i)

      val train_index = ArrayBuffer[Int]()
      for (j <- 1 to cv) {
        if (i != j) {
          train_index.append(j)
        }
      }
      var training = splits(train_index(0))
      for (s <- 1 to train_index.length - 1) {
        training = splits(s).union(training)
      }

      training = training.cache()

      // Run training algorithm to build the model
      val model = new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .run(training)

      // Compute raw scores on the test set.
      val predictionAndLabels = test.map {
        case LabeledPoint(label, features) =>
          val prediction = model.predict(features)
          (prediction, label)
      }

      //两类评价方法
      // Get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(predictionAndLabels)
      val auROC = metrics.areaUnderROC()
      scores.append(auROC)
      println("LR Area under ROC = " + auROC)

      if (auROC > maxAUC) {
        modelweights.clear()
        maxAUC = auROC
        for (w <- model.weights.toArray) {
          modelweights += (w)
        }
      }
    }

    println("max AUC is: " + maxAUC)
    sc.parallelize(modelweights.toSeq).saveAsTextFile(model_weights_path)

    scores.sum / scores.length
  }

  def my_svm_model(sc: SparkContext, filename: String, cv: Int, thr: Double, model_weights_path: String): Double = {

    val ALL = select_feature(sc, filename, thr)
    var modelweights = scala.collection.mutable.ArrayBuffer[Double]()
    var maxAUC = 0.0
    var kflod = ArrayBuffer[Double]()
    var scores = ArrayBuffer[Double]()
    for (i <- 1 to cv) {
      kflod.append(1.0 / cv)
    }

    // Split data into training (60%) and test (40%).
    val splits = ALL.randomSplit(kflod.toArray, seed = 11L)

    for (i <- 0 to cv - 1) {
      val test = splits(i)

      val train_index = ArrayBuffer[Int]()
      for (j <- 1 to cv) {
        if (i != j) {
          train_index.append(j)
        }
      }
      var training = splits(train_index(0))
      for (s <- 1 to train_index.length - 1) {
        training = splits(s).union(training)
      }

      training = training.cache()

      // Run training algorithm to build the model
      val numIterations = 100
      val model = SVMWithSGD.train(training, numIterations)

      // Clear the default threshold.
      model.clearThreshold()

      // Compute raw scores on the test set.
      val scoreAndLabels = test.map { point =>
        val score = model.predict(point.features)
        (score, point.label)
      }

      //两类评价方法
      // Get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      val auROC = metrics.areaUnderROC()
      scores.append(auROC)
      println("SVM Area under ROC = " + auROC)
      if (auROC > maxAUC) {
        modelweights.clear()
        maxAUC = auROC
        for (w <- model.weights.toArray) {
          modelweights += (w)
        }
      }
    }

    println("max AUC is: " + maxAUC)
    sc.parallelize(modelweights.toSeq).saveAsTextFile(model_weights_path)

    scores.sum / scores.length
  }

  def getdata(sc: SparkContext, filename: String) {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //bairong_summary
    val parquetFile = sqlContext.read.parquet("/user/hive/warehouse/bairong_summary")
    //val parquetFile = sqlContext.read.parquet("D:\\Work\\wolaidai\\Droster\\000002_0")

    parquetFile.registerTempTable("parquetFile")
    val bairong_summary_sqlresult = sqlContext.sql("SELECT * FROM parquetFile " + "where m3_maternal_child_supplies_number is not null" +
      " and m3_maternal_child_supplies_pay is not null " + " and m3_daily_necessities_number is not null " +
      " and m3_daily_necessities_pay is not null " + " and m3_household_appliances_number is not null " +
      " and m3_household_appliances_pay is not null " + " and m12_maternal_child_supplies_number is not null " +
      " and m12_maternal_child_supplies_pay is not null" + " and m12_daily_necessities_number is not null" +
      " and m12_daily_necessities_pay is not null " + " and m12_household_appliances_number is not null " +
      " and m12_household_appliances_pay is not null " + " and max_overdue_his_days is not null")

    val result = bairong_summary_sqlresult.map { x =>
      val m3_maternal_child_supplies_number = x(12).toString()
      val m3_maternal_child_supplies_pay = x(13).toString()
      val m3_daily_necessities_number = x(14).toString()
      val m3_daily_necessities_pay = x(15).toString()
      val m3_household_appliances_number = x(16).toString()
      val m3_household_appliances_pay = x(17).toString()
      val m12_maternal_child_supplies_number = x(26).toString()
      val m12_maternal_child_supplies_pay = x(27).toString()
      val m12_daily_necessities_number = x(28).toString()
      val m12_daily_necessities_pay = x(29).toString()
      val m12_household_appliances_number = x(30).toString()
      val m12_household_appliances_pay = x(31).toString()
      val max_overdue_his_days = x(35).toString()
      val application_id = x(36).toString()

      (application_id, (m3_maternal_child_supplies_number, m3_maternal_child_supplies_pay, m3_daily_necessities_number, m3_daily_necessities_pay,
        m3_household_appliances_number, m3_household_appliances_pay, m12_maternal_child_supplies_number, m12_maternal_child_supplies_pay,
        m12_daily_necessities_number, m12_daily_necessities_pay, m12_household_appliances_number, m12_household_appliances_pay, max_overdue_his_days))
    }.reduceByKey { (x, y) =>
      (((x._1.toDouble + y._1.toDouble) / 2).toString(), ((x._2.toDouble + y._2.toDouble) / 2).toString(),
        ((x._3.toDouble + y._3.toDouble) / 2).toString(), ((x._4.toDouble + y._4.toDouble) / 2).toString(),
        ((x._5.toDouble + y._5.toDouble) / 2).toString(), ((x._6.toDouble + y._6.toDouble) / 2).toString(),
        ((x._7.toDouble + y._7.toDouble) / 2).toString(), ((x._8.toDouble + y._8.toDouble) / 2).toString(),
        ((x._9.toDouble + y._9.toDouble) / 2).toString(), ((x._10.toDouble + y._10.toDouble) / 2).toString(),
        ((x._11.toDouble + y._11.toDouble) / 2).toString(), ((x._12.toDouble + y._12.toDouble) / 2).toString(),
        (math.max(x._13.toDouble, y._13.toDouble)).toString())
    }.map { x =>
      x._1 + "," + x._2._1 + "," + x._2._2 + "," + x._2._3 + "," + x._2._4 + "," + x._2._5 + "," + x._2._6 + "," + x._2._7 +
        "," + x._2._8 + "," + x._2._9 + "," + x._2._10 + "," + x._2._11 + "," + x._2._12 + ":" + x._2._13
    }.saveAsTextFile(filename)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("bairongDemo")
    val sc = new SparkContext(conf)
    val filename = "/user/Jeffery/bairong_demo"
    getdata(sc, filename)

    val lr_model_weights_path = "/user/Jeffery/droste/bairongDemo_lr_model_weights_path"
    val svm_model_weights_path = "/user/Jeffery/droste/bairongDemo_svm_model_weights_path"
    my_lr_model(sc, filename, 10, 0.002, lr_model_weights_path)
    my_svm_model(sc, filename, 10, 0.002, svm_model_weights_path)
  }

}
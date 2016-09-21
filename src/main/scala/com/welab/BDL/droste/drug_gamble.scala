package com.welab.BDL.droste

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.regex.Pattern
import com.hankcs.hanlp.dictionary.CustomDictionary
import com.hankcs.hanlp.HanLP
import java.util.List
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.SVMWithSGD
import com.welab.BDL.UserInterests.InformationGain

/**
 * @author LJY
 */
object drug_gamble {
  def ExtraChinese(str: String): String = {
    var value = ""
    //    val p = Pattern.compile("[0-9a-zA-Z\u4e00-\u9fa5]+");
    val p = Pattern.compile("[\u4e00-\u9fa5]+");
    val m = p.matcher(str);

    while (m.find()) {
      if (value.isEmpty())
        value = m.group(0)
      else
        value = value + "," + m.group(0);
    }

    value
  }

  /**
   * HanLP.Config.ShowTermNature = false //不显示词性
   * 可以自动识别中国人名，标注为nr:
   * 可以自动识别音译人名，标注为nrf:
   * 可以自动识别日本人名，标注为nrj:
   * 可以自动识别地名，标注为ns:
   * 可以自动识别机构名，标注为nt:
   */
  def deal_segment(termList: List[Term]): String = {

    var result = ""
    for (i <- 0 to termList.size - 1) {
      var wordwithindex = termList.get(i).toString().split("/")
      //处理英文
      val words = ExtraChinese(wordwithindex(0))

      //处理词性
      val index = wordwithindex(1)
      if (index.startsWith("c") || //过滤连词
        index.startsWith("d") || //过滤副词
        index == "f" || index == "h" || index == "k" || //过滤方位词、前缀、后缀
        index.startsWith("m") || index.startsWith("M") || //过滤数词
        index.startsWith("p") || //过滤介词
        index.startsWith("q") || //过滤量词
        index.startsWith("r") || //过滤代词
        index.startsWith("u") || //过滤助词
        index.startsWith("w") || //过滤标点符号
        words.length() < 2) { //过滤单个词
        //        println("需要过滤的词: "+termList.get(i).toString())
      } else {
        if (result.isEmpty()) {
          result = words
        } else {
          //          result = result + "," + termList.get(i).toString()  //带有词性标注的结果
          result = result + "," + words
          //          println(termList.get(i).toString())
        }
      }

    }
    result
  }

  //根据短信的内容判断是否吸毒用户
  def Isdrug(sc: SparkContext): RDD[(String, Int)] = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //有关吸毒的关键词
    val drugKeys = sc.textFile("/user/Jeffery/keys.txt").collect()
    //val drugKeys = sc.textFile("D:\\Work\\wolaidai\\Droster\\keys.txt").collect()

    val bc = sc.broadcast(drugKeys)

    //短信表
    val parquetFile = sqlContext.read.parquet("/user/hive/warehouse/sms_deduplication_parquet")
    //val parquetFile = sqlContext.read.parquet("D:\\Work\\wolaidai\\Droster\\000002_0")

    parquetFile.registerTempTable("parquetFile")
    val sms_sqlresult = sqlContext.sql("SELECT * FROM parquetFile")

    val sms_result = sms_sqlresult.map { x =>

      //println(x(0))
      //动态添加有关吸毒的分词库
      for (key <- bc.value) {
        CustomDictionary.add(key)
      }

      val segment = HanLP.newSegment().enableNameRecognize(true).enableTranslatedNameRecognize(true)
        .enableJapaneseNameRecognize(true).enablePlaceRecognize(true).enableOrganizationRecognize(true)
        .enablePartOfSpeechTagging(true);
      var termList = segment.seg(x(5).toString()); //对字符串txt进行分词

      val wordslist = deal_segment(termList).split(",")

      var flag = false
      var flag1 = 0; var flag11 = 0
      var flag2 = 0; var flag22 = 0
      var flag3 = 0; var flag222 = 0
      var flag4 = 0; var flag44 = 0
      var flag5 = 0
      for (word <- wordslist) {
        if (word.trim().length() > 1) {
          //println(word)
          //判断是否是通知类吸毒短信
          word.trim() match {
            case "禁毒" => flag1 = 1
            case "提醒" => flag11 = 1
            case "白粉" => flag2 = 1
            case "粉面" => flag22 = 1
            case "陈米" => flag222 = 1
            case "提示" => flag3 = 1
            case "拒绝" => flag4 = 1
            case "毒驾" => flag44 = 1
            case "警方" => flag5 = 1
            case _    =>
          }

          if (bc.value.contains(word.trim())) {
            flag = true
          }
        }
      }

      //println(flag1+" "+flag2+" "+flag3+" "+flag4+" "+flag5)
      if (flag1 + flag11 >= 2 || flag2 + flag22 + flag222 >= 3 || flag3 >= 1 || flag4 + flag44 >= 2 || flag5 >= 1) {
        flag = false
      }

      if (flag) {
        (x(0).toString(), 1)
      } else {
        (x(0).toString(), 0)
      }
    }
    sms_result
  }

  def allclass(sc: SparkContext, filename: String): RDD[(String, ArrayBuffer[Int])] = {
    val data = sc.textFile(filename)
    val result = data.map { x =>
      val fields = x.split("\t")
      var loan_app = 0; var gamble_app = 0; var stock_money_occur = 0;
      var socialapp = 0; var buyerapp = 0; var bookapp = 0; var newsapp = 0
      //借贷类
      var p = Pattern.compile("贷|借钱|借款|分期|金所");
      var m = p.matcher(fields(6));
      if (m.find()) {
        loan_app = 1
      }

      //赌博类
      p = Pattern.compile("彩票|博彩|福彩");
      m = p.matcher(fields(6));
      if (m.find()) {
        gamble_app = 1
      }

      //股票理财类
      p = Pattern.compile("股市|股票|证券|炒股|操盘手|期货|基金|同花顺|贵金属|黄金|大智慧|财|金融|保险|钱包|余额宝");
      m = p.matcher(fields(6));
      if (m.find()) {
        stock_money_occur = 1
      }

      //社交类
      p = Pattern.compile("微信|微博|QQ|人人网|开心网|米聊|facebook|陌陌|朋友网|世纪佳缘|weico|遇见|YY语音|飞聊");
      m = p.matcher(fields(6));
      if (m.find()) {
        socialapp = 1
      }

      //网购类
      p = Pattern.compile("淘宝|天猫|京东|大众点评|淘打折|团购大全|拉手团购|美丽说|豆角优惠|蘑菇街|美团|亚马逊|当当网|苏宁易购|支付宝");
      m = p.matcher(fields(6));
      if (m.find()) {
        buyerapp = 1
      }

      //图书类
      p = Pattern.compile("阅读|书|小说|百阅|开卷有益");
      m = p.matcher(fields(6));
      if (m.find()) {
        bookapp = 1
      }

      //新闻类
      p = Pattern.compile("VIVA畅读|新闻|鲜果联播|掌中新浪|中关村在线|ZAKER");
      m = p.matcher(fields(6));
      if (m.find()) {
        newsapp = 1
      }
      (fields(2), ArrayBuffer(loan_app, gamble_app, stock_money_occur, socialapp, buyerapp, bookapp, newsapp))
    }
    result
  }

  def getdata(sc: SparkContext, save_filename: String) {
    val appfilename = "/user/hive/warehouse/software/importday*"
    //val tempfilename = "D:\\Work\\wolaidai\\AppStatistic\\part0"

    val drug_sms_occur = Isdrug(sc).filter { x => !x._1.isEmpty() }.reduceByKey(_ + _) //1

    val appdata = allclass(sc, appfilename).filter { x => !x._1.isEmpty() }.reduceByKey { (x, y) =>
      val result = ArrayBuffer(0, 0, 0, 0, 0, 0, 0)
      for (i <- 0 to x.length - 1) {
        result(i) = x(i) + y(i)
      }
      result
    } //7
    println(appdata.count())
    //D:\\Work\\wolaidai\\Droster\\passuser.txt
    val account_label = sc.textFile("/user/Jeffery/passuser.txt").map { x =>
      val fields = x.split("\t")
      (fields(0), fields(1).trim().toInt)
    } //1

    //leftOuterJ
    val join1 = account_label.leftOuterJoin(appdata).map { x =>
      val account = x._1
      val label = x._2._1
      var feature = ArrayBuffer(0)
      x._2._2 match {
        case Some(a) => { feature = a }
        case None    => feature = ArrayBuffer(0, 0, 0, 0, 0, 0, 0)
      }

      (account, (feature, label))

    }

    val join2 = join1.leftOuterJoin(drug_sms_occur).map { x =>
      val account = x._1
      val label = x._2._1._2
      val feature1 = x._2._1._1
      var new_feature = 0
      x._2._2 match {
        case Some(a) => new_feature = a
        case None    =>
      }
      feature1.append(new_feature)
      var result = account
      for (f <- feature1) {
        result = result + "," + f.toString()
      }

      result = result + ":" + label.toString()
      result
    }.saveAsTextFile(save_filename)

  }

  //得到boolean类型的训练数据——即判断是否有吸毒，APP各类型即可，不需要计数
  def get_boolean_data(sc: SparkContext, save_filename: String) {
    val appfilename = "/user/hive/warehouse/software/importday*"
    //val tempfilename = "D:\\Work\\wolaidai\\AppStatistic\\part0"

    val drug_sms_occur = Isdrug(sc).filter { x => !x._1.isEmpty() }.reduceByKey(_ + _)

    val appdata = allclass(sc, appfilename).filter { x => !x._1.isEmpty() }.reduceByKey { (x, y) =>
      val result = ArrayBuffer(0, 0, 0, 0, 0, 0, 0)
      for (i <- 0 to x.length - 1) {
        result(i) = x(i) + y(i)
      }
      result
    } //7
    println(appdata.count())
    //D:\\Work\\wolaidai\\Droster\\passuser.txt
    val account_label = sc.textFile("/user/Jeffery/passuser.txt").map { x =>
      val fields = x.split("\t")
      (fields(0), fields(1).trim().toInt)
    } //1

    //leftOuterJ
    val join1 = account_label.leftOuterJoin(appdata).map { x =>
      val account = x._1
      val label = x._2._1
      var feature = ArrayBuffer(0)
      x._2._2 match {
        case Some(a) => { feature = a }
        case None    => feature = ArrayBuffer(0, 0, 0, 0, 0, 0, 0)
      }

      (account, (feature, label))

    }

    val join2 = join1.leftOuterJoin(drug_sms_occur).map { x =>
      val account = x._1
      val label = x._2._1._2
      val feature1 = x._2._1._1
      var new_feature = 0
      try {
        x._2._2 match {
          case Some(a) => new_feature = a
          case None    =>
        }
      } catch {
        case t: Exception => t.printStackTrace() // TODO: handle error
      }

      feature1.append(new_feature)
      var result = account
      for (f <- feature1) {
        if (f > 0) {
          result = result + "," + 1
        } else {
          result = result + "," + 0
        }

      }

      result = result + ":" + label.toString()
      result
    }.saveAsTextFile(save_filename)

  }

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

    val pos_occur = sc.accumulator(0)

    val alldata = data.map { x =>
      val fields = x.split(":")
      val label = fields(1)
      val feature = fields(0).split(",").slice(1, fields(0).split(",").length).map { x => x.toDouble }
      if (label.toInt > 30) {
        pos_occur += 1
        LabeledPoint(1, Vectors.dense(feature))
      } else {
        LabeledPoint(0, Vectors.dense(feature))
      }
    }

    val inforGain_corr = computeGainRate(sc, alldata)
    inforGain_corr._1.foreach { x => println("information gain rate is: " + x) }
    inforGain_corr._2.foreach { x => println("corr whit target variable is: " + x) }

    val all_occur = alldata.count()
    val neg_occur = all_occur - pos_occur.value

    val pos_sample = alldata.filter { x => x.label == 0 }.sample(false, 0.07, 1)

    println("pos sample size is: " + pos_occur.value + "\n" + "neg sample size is: " + neg_occur)

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

  def my_lr_model(sc: SparkContext, filename: String, cv: Int, lr_model_weights_path: String): Double = {

    val ALL = select_feature(sc, filename, 0.002)

    var weights = scala.collection.mutable.ArrayBuffer[Double]()
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
        weights.clear()
        maxAUC = auROC
        for (w <- model.weights.toArray) {
          weights += (w)
        }
      }

    }
    sc.parallelize(weights).saveAsTextFile(lr_model_weights_path)
    scores.sum / scores.length
  }

  def my_svm_model(sc: SparkContext, filename: String, cv: Int, lr_model_weights_path: String): Double = {

    var weights = scala.collection.mutable.ArrayBuffer[Double]()
    var maxAUC = 0.0
    val ALL = select_feature(sc, filename, 0.002)

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
        maxAUC = auROC
        for (w <- model.weights.toArray) {
          weights += (w)
        }
      }

    }

    scores.sum / scores.length
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("drug_gamble")
    val sc = new SparkContext(conf)

    val save_filename = "/user/Jeffery/droste"
    //val save_boolean_filename = "/user/Jeffery/droste_boolean"
    getdata(sc, save_filename)
    //get_boolean_data(sc, save_filename)
    val lr_model_weights_path = "/user/Jeffery/droste/drug_gamble_lr_model_weights_path"

    println("LR's AUC mean is: " + my_lr_model(sc, save_filename, 10, lr_model_weights_path))

    println("SVM's AUC mean is: " + my_svm_model(sc, save_filename, 10, lr_model_weights_path))

  }
}
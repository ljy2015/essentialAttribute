package com.welab.BDL.UserInterests

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.hankcs.hanlp.HanLP
import java.io.File
import scala.io.Source
import java.nio.charset.MalformedInputException
import java.io.PrintWriter
import java.util.List
import java.util.regex.Pattern
import com.hankcs.hanlp.seg.common.Term
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

/**
 * @author LJY
 */
object OneLevelClassification {
  var file_content = ""

  def subdirs(dir: File): Iterator[File] = {

    val children = dir.listFiles.filter(_.isDirectory)

    children.toIterator
  }

  def listfiles(dir: File): Iterator[File] = {
    val children = dir.listFiles.filter { x => x.isFile() }
    children.toIterator
  }
  //  val writer = new PrintWriter(new File("D:\\Work\\out.txt"), "UTF-8")
  def readFile(filename: String): String = {
    if (!file_content.isEmpty()) {
      file_content = ""
    }
    //    var result = ""
    try {
      val lines = Source.fromFile(filename, "GBK").getLines()
      //      println("GBK")
      lines.foreach { x =>
        file_content += x
      }
    } catch {
      case ex: MalformedInputException => {
        try {
          val lines = Source.fromFile(filename, "UTF-8").getLines()
          //          println("UTF-8")
          lines.foreach { x =>
            file_content += x
          }
        } catch {
          case ex: MalformedInputException => {

            file_content = readfile.readTxt(filename)
            //            writer.write(file_content + "\n")
            //            writer.flush()
            println(file_content)
          }
          case ex: Exception => println("文件有问题！")
        }
      }
      case ex: Exception => println("文件有问题！！！")
    }

    file_content
  }

  def writefile(content: String, filename: String) {
    val writer = new PrintWriter(new File(filename), "UTF-8")
    writer.append(content) //write(content)
    writer.close
  }

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

  def deal_train_data(filename: String): ArrayBuffer[(String, String)] = {

    var result = new scala.collection.mutable.ArrayBuffer[(String, String)]()

    val segment = HanLP.newSegment().enableNameRecognize(true).enableTranslatedNameRecognize(true)
      .enableJapaneseNameRecognize(true).enablePlaceRecognize(true).enableOrganizationRecognize(true)
      .enablePartOfSpeechTagging(true);

    subdirs(new File(filename)).foreach { x =>
      val label = x.getName()
      println(label)

      listfiles(x).foreach { file =>

        println(file.getAbsolutePath)
        val txt = readFile(file.getAbsolutePath)

        var termList = segment.seg(txt); //对字符串txt进行分词
        result.+=((deal_segment(termList), label))
      }

    }

    result
  }

  //将所有文档的数据进行存储
  def save_all_data(sc: SparkContext, train_filename: String, test_filename: String, save_train_filename: String, save_test_filename: String) {
    val traindata = deal_train_data(train_filename)

    println("train size is " + traindata.length)
    val traindata_rdd = sc.parallelize(traindata, 100).map { x => x._1 + "::" + x._2 }.saveAsTextFile(save_train_filename)

    val testdata = deal_train_data(test_filename)
    println("test size is " + traindata.length)
    val testdata_rdd = sc.parallelize(testdata, 100).map { x => x._1 + "::" + x._2 }.saveAsTextFile(save_test_filename)
  }

  def feature_alldata(sc: SparkContext, filename: String): RDD[String] = {
    val data = sc.textFile(filename, 1000)

    //计算所有的特征词
    val all_feature = data.map { x =>
      val wds = x.split("::")(0)
      val fields = wds.split(",")
      val doc_words = new scala.collection.mutable.HashSet[String]()

      for (field <- fields) {
        if (!doc_words.contains(field)) {
          doc_words.add(field)
        }
      }
      var result = ""
      for (wd <- doc_words) {
        if (result.isEmpty()) {
          result = wd
        } else {
          result = result + "," + wd
        }
      }
      result
    }.flatMap { x => x.split(",") }.map { x => (x, 1) }
      .reduceByKey(_ + _).map { x => x._1 } //所有词对应的IDF

    all_feature
  }

  //直接计算得到每个特征词的tf_idf，不保留特征词
  def TF_IDF_new(sc: SparkContext, filename: String, feature_alldata: Array[String]): RDD[String] = {
    println(filename)
    val data = sc.textFile(filename, 1000)
    val doc_num = data.count();

    //计算IDF
    val idf = data.map { x =>
      val wds = x.split("::")(0)
      val fields = wds.split(",")
      val doc_words = new scala.collection.mutable.HashSet[String]()

      for (field <- fields) {
        if (!doc_words.contains(field)) {
          doc_words.add(field)
        }
      }
      var result = ""
      for (wd <- doc_words) {
        if (result.isEmpty()) {
          result = wd
        } else {
          result = result + "," + wd
        }
      }
      result
    }.flatMap { x => x.split(",") }.map { x => (x, 1) }
      .reduceByKey(_ + _).map { x => (x._1, math.log10(doc_num.toDouble / x._2.toDouble)) }.collect() //所有词对应的IDF

    //将idf变成map，方便查询 
    val idf_map = new scala.collection.mutable.HashMap[String, Double]()
    for (d <- idf) {
      idf_map.put(d._1, d._2)
    }
    //将idf广播出去
    val idf_broad = sc.broadcast(idf_map)

    //计算TF
    val TFIDF = data.map { x =>
      println(x)
      val words = x.split("::")(0)
      val label = x.split("::")(1).toInt
      val fields = words.split(",")
      val words_num = new scala.collection.mutable.HashMap[String, Int]() //每个词对应的频率
      var all_words_num = 0 //所有词个数
      for (field <- fields) {
        all_words_num += 1
        if (words_num.contains(field)) {
          words_num(field) += 1
        } else {
          words_num.put(field, 1)
        }
      }

      //val words_tf_idf = new scala.collection.mutable.HashMap[String, Double]()
      var result = ""

      for (word <- feature_alldata) { //统一所有的词
        var idf = 0.0
        var tf = 0.0;
        if (words_num.contains(word)) { //如果当前文档中包含了一些词
          idf_broad.value.get(word) match {
            case None    =>
            case Some(a) => idf = a
          }

          words_num.get(word) match {
            case None =>
            case Some(a) => {
              val temp = a
              tf = temp / all_words_num.toDouble
            }
          }

        }
        if (result.isEmpty()) {
          result = (tf * idf).toString()
        } else {
          result = result + "," + (tf * idf)
        }
        //words_tf_idf.put(word, (tf * idf))
      }

      /*for (word <- words_num) {
        val tf = word._2.toDouble / all_words_num.toDouble
        var idf=0.1
        if(idf_broad.value.contains(word._1)){
          idf_broad.value.get(word._1) match{
            case None=>
            case Some(a)=>idf=a
          }
        }
        words_tf_idf.put(word._1, tf*idf)
      }*/

      //(words_tf_idf, label)
      result + ":" + label
    }
    TFIDF
  }

  def TF_IDF(sc: SparkContext, filename: String, feature_alldata: Array[String]): RDD[(HashMap[String, Double], Int)] = {
    println(filename)
    val data = sc.textFile(filename, 1000)
    val doc_num = data.count();

    //计算IDF
    val idf = data.map { x =>
      val wds = x.split("::")(0)
      val fields = wds.split(",")
      val doc_words = new scala.collection.mutable.HashSet[String]()

      for (field <- fields) {
        if (!doc_words.contains(field)) {
          doc_words.add(field)
        }
      }
      var result = ""
      for (wd <- doc_words) {
        if (result.isEmpty()) {
          result = wd
        } else {
          result = result + "," + wd
        }
      }
      result
    }.flatMap { x => x.split(",") }.map { x => (x, 1) }
      .reduceByKey(_ + _).map { x => (x._1, math.log10(doc_num.toDouble / x._2.toDouble)) }.collect() //所有词对应的IDF

    //将idf变成map，方便查询 
    val idf_map = new scala.collection.mutable.HashMap[String, Double]()
    for (d <- idf) {
      idf_map.put(d._1, d._2)
    }
    //将idf广播出去
    val idf_broad = sc.broadcast(idf_map)

    //计算TF
    val TFIDF = data.map { x =>
      println(x)
      val words = x.split("::")(0)
      val label = x.split("::")(1).toInt
      val fields = words.split(",")
      val words_num = new scala.collection.mutable.HashMap[String, Int]() //每个词对应的频率
      var all_words_num = 0 //所有词个数
      for (field <- fields) {
        all_words_num += 1
        if (words_num.contains(field)) {
          words_num(field) += 1
        } else {
          words_num.put(field, 1)
        }
      }

      val words_tf_idf = new scala.collection.mutable.HashMap[String, Double]()
      for (word <- feature_alldata) { //统一所有的词
        var idf = 0.0
        var tf = 0.0;
        if (words_num.contains(word)) { //如果当前文档中包含了一些词
          idf_broad.value.get(word) match {
            case None    =>
            case Some(a) => idf = a
          }

          words_num.get(word) match {
            case None =>
            case Some(a) => {
              val temp = a
              tf = temp / all_words_num.toDouble
            }
          }

        }
        words_tf_idf.put(word, (tf * idf))
      }

      /*for (word <- words_num) {
        val tf = word._2.toDouble / all_words_num.toDouble
        var idf=0.1
        if(idf_broad.value.contains(word._1)){
          idf_broad.value.get(word._1) match{
            case None=>
            case Some(a)=>idf=a
          }
        }
        words_tf_idf.put(word._1, tf*idf)
      }*/

      (words_tf_idf, label)
    }
    TFIDF
  }

  def NaiveBayesMethod(sc: SparkContext, traindata: RDD[LabeledPoint], testdata: RDD[LabeledPoint]) {

    val model = NaiveBayes.train(traindata, lambda = 1.0, modelType = "multinomial")
    model.save(sc, "/user/Jeffery/userInterest/model")

    //val predictionAndLabel = testdata.take(1).map(p => (model.predict(p.features), p.label)).foreach(x => println(x))

    /*for(i <- 1 to 32){
      val accuracy =(1.0 * predictionAndLabel.filter(x => x._1 == i && x._2==i).count() / testdata.filter { x => x.label==i }.count())
      println("the "+i+"th's accuracy is : "+accuracy)
    }*/
    //val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testdata.count()

    // Save and load model
    //sc.parallelize(model.theta).saveAsTextFile("/user/Jeffery/userInterest/model_theta_weight")
    //sc.parallelize(model.pi).saveAsTextFile("/user/Jeffery/userInterest/model_pi_weight")
    //model.save(sc, "target/tmp/myNaiveBayesModel")
    //val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
  }

  def NaiveBayesMethod(sc: SparkContext, traindata: String, testdata: String) {
    val train = sc.textFile(traindata).map { x =>
      val fields = x.split(":")
      val value = fields(0).split(",").map { x => x.toDouble }
      val label = fields(1).toInt
      LabeledPoint(label, Vectors.dense(value))
    }
    val test = sc.textFile(testdata).map { x =>
      val fields = x.split(":")
      val value = fields(0).split(",").map { x => x.toDouble }
      val label = fields(1).toInt
      LabeledPoint(label, Vectors.dense(value))
    }

    val model = NaiveBayes.train(train, lambda = 1.0, modelType = "multinomial")
    model.save(sc, "/user/Jeffery/userInterest/model")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))

    for (i <- 1 to 32) {
      val accuracy = (1.0 * predictionAndLabel.filter(x => x._1 == i && x._2 == i).count() / test.filter { x => x.label == i }.count())
      println("the " + i + "th's accuracy is : " + accuracy)
    }
    //val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testdata.count()

    // Save and load model
    //    sc.parallelize(model.theta).saveAsTextFile("/user/Jeffery/userInterest/model_theta_weight")
    //    sc.parallelize(model.pi).saveAsTextFile("/user/Jeffery/userInterest/model_pi_weight")
    //model.save(sc, "target/tmp/myNaiveBayesModel")
    //val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
  }

  //将tf-idf的结果进行保存了。
  def data_TF_IDF(sc: SparkContext, filename: String, allfeature: Array[String], save_tfidf_path: String) {
    val traindata_TF_IDF = TF_IDF(sc, filename, allfeature).map { x =>
      var temp = ""
      for (word <- x._1) {
        if (temp.isEmpty()) {
          temp = word._2.toString()
        } else {
          temp = temp + "," + word._2
        }
      }
      temp = temp + ":" + x._2
      temp
    }.saveAsTextFile(save_tfidf_path)

  }

  def train(sc: SparkContext, filename: String, feature_alldata: Array[String], save_model_path: String) {
    val traindata_TF_IDF = TF_IDF(sc, filename, feature_alldata).map { x =>
      val temp = scala.collection.mutable.ArrayBuffer[Double]()
      for (word <- x._1) {
        temp.append(word._2)
      }
      LabeledPoint(x._2, Vectors.dense(temp.toArray))
    }

    val model = NaiveBayes.train(traindata_TF_IDF, lambda = 1.0, modelType = "multinomial")
    model.save(sc, save_model_path)
  }

  def predict(sc: SparkContext, model_path: String,testdata:RDD[LabeledPoint]) {
    val model = NaiveBayesModel.load(sc, model_path)
    val predictionAndLabel = testdata.map(p => (model.predict(p.features), p.label,p))
    
    val accuracy_total = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testdata.count()
    println("accuracy_total is : "+accuracy_total)
    for (i <- 1 to 32) {
      val accuracy = (1.0 * predictionAndLabel.filter(x => x._1 == i && x._2 == i).count().toDouble / testdata.filter { x => x.label == i }.count())
      println("the " + i + "th's accuracy is : " + accuracy)
    }
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OneLevelClassification")
    val sc = new SparkContext(conf)

    //val data=sc.textFile("D:\\Work\\wolaidai\\用户兴趣模型\\alldata")

    //保存一下中间结果
    val train_filename = "/disk1/jobs/Jeffery/userInteresting/data/traindata"
    //val train_filename = args(0)
    val test_filename = "/disk1/jobs/Jeffery/userInteresting/data/testdata"
    //val test_filename = args(1)
    val save_train_filename = "/user/Jeffery/userInterest/train_data"
    //val save_train_filename =args(2)
    val save_test_filename = "/user/Jeffery/userInterest/test_data"
    //val save_test_filename = args(3)
    //save_all_data(sc, train_filename, test_filename, save_train_filename, save_test_filename) //这个只要执行一次就行

    //计算训练集所有的特征
    val all_feature_save_path = "/user/Jeffery/userInterest/all_feature"
    val all_feature = feature_alldata(sc, save_train_filename)
    //all_feature.saveAsTextFile(all_feature_save_path)//保存所有的特征词
    val allfeature = all_feature.collect()

    println("all_feature size is:" + allfeature.length)
    
    val modelpath="/user/Jeffery/userInterest/model"
    val local_model_path="/disk1/jobs/Jeffery/userInteresting/model"
//    train(sc,save_train_filename,allfeature,local_model_path)

    //val train_save_tfidf_path = "/user/Jeffery/userInterest/train_data_TFIDF"
    //data_TF_IDF(sc, save_train_filename, allfeature, train_save_tfidf_path)
    //TF_IDF_new(sc, save_train_filename, allfeature).saveAsTextFile(train_save_tfidf_path)

    val testdata = TF_IDF(sc, save_test_filename, allfeature).map { x =>
      val temp = scala.collection.mutable.ArrayBuffer[Double]()
      for (word <- x._1) {
        temp.append(word._2)
      }
      LabeledPoint(x._2, Vectors.dense(temp.toArray))
    }

    //val test_save_tfidf_path = "/user/Jeffery/userInterest/test_data_TFIDF"
    //data_TF_IDF(sc, save_train_filename, all_feature, test_save_tfidf_path)
    //TF_IDF_new(sc, save_train_filename, allfeature).saveAsTextFile(test_save_tfidf_path)

    predict(sc,modelpath,testdata)
    //NaiveBayesMethod(sc, traindata_TF_IDF, testdata)
    //NaiveBayesMethod(sc, train_save_tfidf_path, test_save_tfidf_path)

  }
}
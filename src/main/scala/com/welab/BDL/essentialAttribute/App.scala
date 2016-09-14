package com.welab.BDL.essentialAttribute

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.util.MLUtils

/**
 * @author ${user.name}
 */
object App {

  def subdirs(dir: File): Iterator[File] = {

    val children = dir.listFiles.filter(_.isDirectory)

    children.toIterator
  }

  def filedirs(dir: File): Iterator[File] = {
    val children = dir.listFiles.filter { x => x.isFile() }
    children.toIterator
  }

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TextClassification").setMaster("local")
    val sc = new SparkContext(conf)

    val file = new File("D:\\Work\\wolaidai\\中文语料库\\new")

    var acc = sc.accumulator(0)

    val alldata = sc.textFile("D:\\Work\\wolaidai\\中文语料库\\all_files.txt", 10).distinct().collect()
    val words = sc.broadcast(alldata)

    subdirs(file).foreach { x =>
      val label = x.getName
      val data = sc.textFile(x.getAbsolutePath)
      data.map { x => (x, 1) }.reduceByKey(_ + _).map { x =>
        (x, x._2.toDouble / data.count())
      }

    }

    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    // Save and load model
    model.save(sc, "target/tmp/myNaiveBayesModel")
    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")

  }

}

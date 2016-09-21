package com.welab.BDL.UserInterests

import com.hankcs.hanlp.tokenizer.NLPTokenizer
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.HanLP
import java.util.regex.Pattern
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.Parallel
import java.io.File
import scala.io.Source
import java.nio.charset.MalformedInputException
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

/**
 * @author LJY
 */
object test {
  var file_content = ""
  def subdirs(dir: File): Iterator[File] = {

    val children = dir.listFiles.filter(_.isDirectory)

    children.toIterator
  }

  def listfiles(dir: File): Iterator[File] = {
    val children = dir.listFiles.filter { x => x.isFile() }
    children.toIterator
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
  def main(args: Array[String]): Unit = {
    /*    //    HanLP.Config.ShowTermNature = false //不显示词性
    val termList = NLPTokenizer.segment("中国科学院计算技术研究所的宗成庆教授正在教授自然语言处理课程");
    //    println(termList);

    val testCase = Array[String]("中niaho国科学院hellonihao计算技术研究所")
    val segment = HanLP.newSegment().enableOrganizationRecognize(true);
    for (sentence <- testCase) {
      val termList = segment.seg(sentence);
      for (i <- 0 to termList.size() - 1) {
        println(ExtraChinese(termList.get(i).toString()))
      }
    }

    println("rt".startsWith("r"))

    val conf = new SparkConf().setAppName("OneLevelClassification").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("D:\\Work\\wolaidai\\用户兴趣模型\\alldata")
    println(data.count())*/
    /*val filename = "D:\\Work\\wolaidai\\用户兴趣模型\\data\\traindata"
    subdirs(new File(filename)).foreach { x =>
      val label = x.getName()

      listfiles(x).foreach { file =>
        println(file.getAbsolutePath)
        val txt = readFile(file.getAbsolutePath)
        println(txt)
      }
      //println("the " + label + " file count is :" + occur)
    }*/

/*    val ss = Array(1, 2, 3, 4)
    val conf = new SparkConf().setAppName("OneLevelClassification").setMaster("local")
    val sc = new SparkContext(conf)*/
    //sc.parallelize(Array(LabeledPoint(1,Vectors.dense(Array(1.0,2.0,3.0))))).saveAsTextFile("D:\\Work\\wolaidai\\用户兴趣模型\\test")
    println("你好你好nihao".length())
    
  }
}
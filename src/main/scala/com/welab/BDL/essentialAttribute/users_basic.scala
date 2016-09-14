package com.welab.BDL.essentialAttribute

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * @author LJY
 */
object users_basic {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("essentialAttribute").setMaster("local")
    val sc=new SparkContext(conf)
    
    val hiveContext = new HiveContext(sc)
    
    val data=hiveContext.sql("select * from users_basic where profile_position_id is null limit 10")
    data.foreach { x => println( x(19))
      println(x.length)  
      
    }
    
  }
}
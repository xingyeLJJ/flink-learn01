package com.atguigu.service

import com.atguigu.Function.UVProcessWindowFunction
import com.atguigu.bean.UserBehavior
import com.atguigu.dao.UniqueVisitorAnalysisDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

class UniqueVisitorAnalysisService {


  private val uniqueVisitorAnalysisDao: UniqueVisitorAnalysisDao = new UniqueVisitorAnalysisDao


  def getUV(filePath: String) = {
    val dataDS: DataStream[String] = uniqueVisitorAnalysisDao.readTextFile(filePath)


    val userBehaviorDS = dataDS.map(data => {
      val datas: Array[String] = data.split(",")
      UserBehavior(datas(0).toLong, datas(1).toLong, datas(2).toInt, datas(3), datas(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    userBehaviorDS
      .filter(_.behavior == "pv")
      .map(data => ("pv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .process(new UVProcessWindowFunction)

  }
}

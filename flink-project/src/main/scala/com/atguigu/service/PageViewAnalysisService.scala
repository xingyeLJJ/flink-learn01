package com.atguigu.service

import com.atguigu.bean.UserBehavior
import com.atguigu.dao.PageViewAnalysisDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

class PageViewAnalysisService {


  private val pageViewAnalysisDao: PageViewAnalysisDao = new PageViewAnalysisDao

  def getPV(filePath: String) = {
    val dataDS: DataStream[String] = pageViewAnalysisDao.readTextFile(filePath)

    val userBehaviorDS = dataDS.map(data => {
      val datas: Array[String] = data.split(",")
      UserBehavior(datas(0).toLong, datas(1).toLong, datas(2).toInt, datas(3), datas(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)


    userBehaviorDS
      .filter(_.behavior == "pv")
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

  }
}

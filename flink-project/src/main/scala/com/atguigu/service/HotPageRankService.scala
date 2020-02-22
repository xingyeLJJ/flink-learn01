package com.atguigu.service

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.atguigu.Function.{HotPageRankAggregateFunction, HotPageRankKeyedProcessFunction, HotPageRankWindowFunction}
import com.atguigu.bean
import com.atguigu.bean.{ApacheLogEvent, UrlViewCount}
import com.atguigu.dao.HotPageRankDao
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class HotPageRankService {
  private val hotPageRankDao: HotPageRankDao = new HotPageRankDao

  def getHotPageRank(num: Int, filePath: String) = {
    // TODO 1. 获取服务器的时间
    val dataDS: DataStream[String] = hotPageRankDao.readTextFile(filePath)
    // 17/05/2015:10:05:50
    val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")

    // TODO 2. 将从服务器中获取到的数据封装到ApacheLogEvent对象中
    // TODO 3. 从数据抽取事件时间
    val apacheLogEventDS: DataStream[ApacheLogEvent] = dataDS.map(data => {
      val datas: Array[String] = data.split(" ")
      val ts = sdf.parse(datas(3)).getTime
      ApacheLogEvent(datas(0), datas(1), ts, datas(5), datas(6))
    })
      //      .assignAscendingTimestamps(_.eventTime)
      .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.minutes(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          element.eventTime
        }
      }
    )

    val UrlViewCountDS: DataStream[UrlViewCount] = apacheLogEventDS
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new HotPageRankAggregateFunction, new HotPageRankWindowFunction)

    UrlViewCountDS.keyBy(_.windowEnd).process(
      new HotPageRankKeyedProcessFunction(num)
    )


  }
}

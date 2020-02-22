package com.atguigu.service

import com.atguigu.bean
import com.atguigu.bean.{AppMarketCount, AppMarketData}
import com.atguigu.dao.AppMarketingByChannelAnalysisDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.runtime.aggregate.TimeWindowPropertyCollector
import org.apache.flink.util.Collector


class AppMarketingByChannelAnalysisService {


  private val appMarketingByChannelAnalysisDao: AppMarketingByChannelAnalysisDao = new AppMarketingByChannelAnalysisDao

  def getAnalysisDatas() = {
    // TODO 1. 获取推广数据
    val appMarketDataDS: DataStream[bean.AppMarketData] = appMarketingByChannelAnalysisDao.mockData()

    // TODO 2. 抽取事件时间
    val tsDS: DataStream[bean.AppMarketData] = appMarketDataDS.assignAscendingTimestamps(_.timestamp)

    // TODO 3.将数据根据渠道和行为进行分区
    // TODO 如果是根据多个字段进行分区 ,可以使用下面的那两种方法
    // TODO （字段 + 字段）
    // TODO 字段 + "_" + 字段
    //    tsDS.keyBy("marketing", "behavior")
    val dataKS: KeyedStream[bean.AppMarketData, String] = tsDS.keyBy(data => {
      data.marketing + "_" + data.behavior
    })

    // TODO 4. 时间窗口
    dataKS.timeWindow(Time.seconds(10))
      .process(new ProcessWindowFunction[AppMarketData, AppMarketCount, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[AppMarketData], out: Collector[AppMarketCount]): Unit = {
          out.collect(AppMarketCount(key.split("_")(0), key.split("_")(1), elements.size))
        }
      })

  }
}

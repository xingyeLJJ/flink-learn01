package com.atguigu.service

import com.atguigu.Function.{HotItemRankAggregateFunction, HotItemRankProcessFunction, HotItemRankWindowFunction}
import com.atguigu.bean
import com.atguigu.bean.UserBehavior
import com.atguigu.dao.HotItemRankDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotItemRankService {


  private val hotItemRankDao: HotItemRankDao = new HotItemRankDao

  def getHotItemRank(num: Int, filePath: String) = {
    // TODO 1. 读取文件数据
    val dataDS: DataStream[String] = hotItemRankDao.readTextFile(filePath)

    // TODO 2. 将读取的数据进行结构的转换
    // TODO 3. 从数据中抽取事件时间
    val userBehaviorDS: DataStream[UserBehavior] = dataDS.map(data => {
      val datas: Array[String] = data.split(",")
      UserBehavior(datas(0).toLong, datas(1).toLong, datas(2).toInt, datas(3), datas(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    // TODO 4. 过滤数据
    // TODO 5. 根据itemId进行分区
    // TODO 6. 加上滑动窗口
    val userBehaviorWS: WindowedStream[UserBehavior, Long, TimeWindow] = userBehaviorDS.filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))


    // TODO 7. 将窗口中的数据进行聚合
    val itemClickCountDS: DataStream[bean.ItemClickCount] =
      userBehaviorWS.aggregate(new HotItemRankAggregateFunction, new HotItemRankWindowFunction)


    // TODO 8. 根据windowEndTime进行分区
    val itemClickCountKS: KeyedStream[bean.ItemClickCount, Long] = itemClickCountDS.keyBy(_.windowEndTime)

    // TODO 9. 对每个分区进行倒序排序，取出前三
    itemClickCountKS.process(new HotItemRankProcessFunction(num))

  }


}

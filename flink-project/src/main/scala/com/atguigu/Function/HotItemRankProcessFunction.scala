package com.atguigu.Function

import java.lang
import java.sql.Timestamp

import com.atguigu.bean.ItemClickCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class HotItemRankProcessFunction(num: Int) extends ProcessFunction[ItemClickCount, String] {

  private var itemCountList: ListState[ItemClickCount] = _

  private var calcTime: ValueState[Long] = _


  override def open(parameters: Configuration): Unit = {
    itemCountList = getRuntimeContext.getListState[ItemClickCount](
      new ListStateDescriptor[ItemClickCount]("itemCountList", classOf[ItemClickCount])
    )

    calcTime = getRuntimeContext.getState[Long](
      new ValueStateDescriptor[Long]("calcTime", classOf[Long])
    )

  }

  override def processElement(value: ItemClickCount,
                              ctx: ProcessFunction[ItemClickCount, String]#Context,
                              out: Collector[String]): Unit = {

    itemCountList.add(value)

    if (calcTime.value() == 0L) {
      ctx.timerService().registerEventTimeTimer(value.windowEndTime)
      calcTime.update(value.windowEndTime)
    }
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[ItemClickCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 从状态对象中获取数据
    val counts: lang.Iterable[ItemClickCount] = itemCountList.get()
    val buffer: ListBuffer[ItemClickCount] = new ListBuffer[ItemClickCount]()

    counts.iterator().foreach(item => {
      buffer.append(item)
    })
    itemCountList.clear()
    // 对数据进行排序
    val iccList: ListBuffer[ItemClickCount] = buffer.sortBy(-_.count).take(num)

    val builder = new StringBuilder()
    builder.append("时间范围：" + new Timestamp(timestamp) + "\n")
    iccList.foreach(
      data => {
        builder.append(s"商品ID：${data.itemId},点击次数为：${data.count} \n")
      }
    )

    builder.append("=======================================")
    // 间隔刷屏
    Thread.sleep(500)
    out.collect(builder.toString())
  }
}

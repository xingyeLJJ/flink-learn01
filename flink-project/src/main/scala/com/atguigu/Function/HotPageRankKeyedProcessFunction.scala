package com.atguigu.Function

import java.lang
import java.sql.Timestamp

import com.atguigu.bean.UrlViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class HotPageRankKeyedProcessFunction(num: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

  private var urlDatas: ListState[UrlViewCount] = _

  private var alarmTime: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    urlDatas = getRuntimeContext.getListState[UrlViewCount](
      new ListStateDescriptor[UrlViewCount]("listState", classOf[UrlViewCount])
    )
    alarmTime = getRuntimeContext.getState[Long](
      new ValueStateDescriptor[Long]("alarmTime", classOf[Long])
    )
  }

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlDatas.add(value)
    if (alarmTime.value() == 0L) {
      // 注册定时器
      ctx.timerService().registerEventTimeTimer(value.windowEnd)
      alarmTime.update(value.windowEnd)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val list: lang.Iterable[UrlViewCount] = urlDatas.get()
    val buffer: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
    list.foreach(info => buffer.add(info))

    val orderList: ListBuffer[UrlViewCount] = buffer.sortBy(_.count)(Ordering.Long.reverse).take(num)

    val builder: StringBuilder = new StringBuilder

    builder.append("时间范围：" + new Timestamp(timestamp) + "\n")
    orderList.foreach(
      data => {
        builder.append(s"浏览的页面：${data.url},浏览次数为：${data.count} \n")
      }
    )

    builder.append("=======================================")
    // 间隔刷屏
    Thread.sleep(500)
    out.collect(builder.toString())

  }

}

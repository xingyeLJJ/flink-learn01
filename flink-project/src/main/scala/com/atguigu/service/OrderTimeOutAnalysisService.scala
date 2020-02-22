package com.atguigu.service

import java.util

import com.atguigu.bean.OrderLogData
import com.atguigu.dao.OrderTimeOutAnalysisDao
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class OrderTimeOutAnalysisService {


  private val orderTimeOutAnalysisDao: OrderTimeOutAnalysisDao = new OrderTimeOutAnalysisDao

  /**
    * 使用CEP完成业务
    *
    * @param filePath
    * @return
    */
  def getOrderTimeOutDatasByCEP(filePath: String) = {
    val dataDS: DataStream[String] = orderTimeOutAnalysisDao.readTextFile(filePath)

    val OrderLogDataDS: DataStream[OrderLogData] = dataDS.map(data => {
      val datas: Array[String] = data.split(",")
      OrderLogData(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    // TODO 根据订单id进行分组
    val orderKS: KeyedStream[OrderLogData, Long] = OrderLogDataDS.keyBy(_.orderId)


    val p: Pattern[OrderLogData, OrderLogData] = Pattern.begin[OrderLogData]("begin")
      .where(_.status == "create")
      .followedBy("followedBy")
      .where(_.status == "pay")
      .within(Time.minutes(15))

    val ps: PatternStream[OrderLogData] = CEP.pattern(orderKS, p)
    val orderTimeOutTag: OutputTag[String] = new OutputTag[String]("orderTimeOutTag")
    ps.select(orderTimeOutTag, new PatternTimeoutFunction[OrderLogData, String] {
      override def timeout(pattern: util.Map[String, util.List[OrderLogData]], timeoutTimestamp: Long): String = {
        pattern.toString
      }
    }, new PatternSelectFunction[OrderLogData, String] {
      override def select(pattern: util.Map[String, util.List[OrderLogData]]): String = {
        pattern.toString
      }
    }
    )

    /**
      * 不使用CEP完成该业务
      *
      * @param filePath
      */
    def getOrderTimeOutDatas(filePath: String) = {
      val dataDS: DataStream[String] = orderTimeOutAnalysisDao.readTextFile(filePath)

      val OrderLogDataDS: DataStream[OrderLogData] = dataDS.map(data => {
        val datas: Array[String] = data.split(",")
        OrderLogData(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000)

      // TODO 根据订单id进行分组
      val orderKS: KeyedStream[OrderLogData, Long] = OrderLogDataDS.keyBy(_.orderId)

      val unmatchTag: OutputTag[String] = new OutputTag[String]("unmatchTag")

      orderKS.process(new KeyedProcessFunction[Long, OrderLogData, String] {

        private var payFlag: ValueState[Boolean] = _
        private var alarmTime: ValueState[Long] = _


        override def open(parameters: Configuration): Unit = {
          payFlag = getRuntimeContext.getState[Boolean](
            new ValueStateDescriptor[Boolean]("payFlag", classOf[Boolean])
          )
          alarmTime = getRuntimeContext.getState[Long](
            new ValueStateDescriptor[Long]("alarmTime", classOf[Long])
          )
        }


        override def processElement(value: OrderLogData, ctx: KeyedProcessFunction[Long, OrderLogData, String]#Context, out: Collector[String]): Unit = {
          // TODO 判断来的数据是什么
          if (value.status == "create") { // TODO 如果来的数据是create
            // TODO 判断是否已经支付了
            if (payFlag.value()) { // TODO 如果这次来的是订单创建的数据，并且支付数据也已经来了
              // TODO 删除等待订单创建的定时器
              ctx.timerService().deleteEventTimeTimer(alarmTime.value())
              // TODO 重置数据
              payFlag.clear()
              alarmTime.clear()
              // TODO 将数据输出
              out.collect("订单：" + ctx.getCurrentKey + "已经完成订单的创建和支付")
            } else { // TODO 如果这次来的是订单创建的数据，支付数据还没来
              // TODO 创建定时器
              val payTs: Long = value.timestamp * 10000 + 15 * 60 * 1000L
              ctx.timerService().registerEventTimeTimer(payTs)
              alarmTime.update(payTs)
            }
          } else if (value.status == "pay") { // TODO 如果来的数据是pay
            // TODO 判断有没有先到
            if (alarmTime.value() == 0L) { // TODO 如果订单创建数据还没先到
              // TODO 注册一个等待订单创建的定时器
              val orderTs: Long = value.timestamp * 1000 + 3 * 60 * 1000
              ctx.timerService().registerEventTimeTimer(orderTs)
              alarmTime.update(orderTs)
              // 更新支付状态
              payFlag.update(true)
            } else { // TODO 订单创建数据先到
              // TODO 删除定时器
              ctx.timerService().deleteEventTimeTimer(alarmTime.value())
              // TODO 输出
              out.collect("订单：" + ctx.getCurrentKey + "已经完成订单的创建和支付")
              // TODO 重置数据
              // TODO 其实不需要重置payFlag的数据，本来这个数据没有改变过
              alarmTime.clear()
            }
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderLogData, String]#OnTimerContext, out: Collector[String]): Unit = {
          if (payFlag.value()) { // 已经支付了，但是订单创建数据在指定的时间没有到达
            ctx.output(unmatchTag, "订单：" + ctx.getCurrentKey + "已经支付了，但是未获取到创建订单的数据")
          } else {
            ctx.output(unmatchTag, "订单：" + ctx.getCurrentKey + "创建成功了，但是未获取到支付订单的数据")
          }
          payFlag.clear()
          alarmTime.clear()
        }
      })

    }


  }
}

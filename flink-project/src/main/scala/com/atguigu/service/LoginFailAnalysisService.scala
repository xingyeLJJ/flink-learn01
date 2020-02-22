package com.atguigu.service

import java.util

import com.atguigu.bean.LoginLogData
import com.atguigu.dao.LoginFailAnalysisDao
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class LoginFailAnalysisService {


  private val loginFailAnalysisDao: LoginFailAnalysisDao = new LoginFailAnalysisDao


  def getLoginFailByCEP(failCount: Int, filePath: String) = {
    // TODO 1. 获取数据
    val dataDS: DataStream[String] = loginFailAnalysisDao.readTextFile(filePath)
    // TODO 2. 处理数据
    val loginLogDataDS = dataDS.map(data => {
      val datas: Array[String] = data.split(",")
      LoginLogData(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
    })
      // TODO 3. 抽取事件时间，并且设置watermark
      .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LoginLogData](Time.seconds(10)) {
        override def extractTimestamp(element: LoginLogData): Long = {
          element.timestamp * 1000
        }
      }
    )
    val loginLogDataKS: KeyedStream[LoginLogData, Long] = loginLogDataDS.keyBy(_.userId)

    // TODO 声明CEP规则
    val p = Pattern.begin[LoginLogData]("begin")
      .where(_.status == "fail")
      .next("next")
      .where(_.status == "fail")
      //        .times(failCount - 2)
      .within(Time.seconds(2))

    // TODO 应用规则
    val ps: PatternStream[LoginLogData] = CEP.pattern(loginLogDataKS, p)

    // TODO 输出
    ps.select(new PatternSelectFunction[LoginLogData, String] {
      override def select(pattern: util.Map[String, util.List[LoginLogData]]): String = {
        pattern.toString
      }
    })
  }


  def getLoginFail(failCount: Int, filePath: String) = {
    // TODO 1. 获取数据
    val dataDS: DataStream[String] = loginFailAnalysisDao.readTextFile(filePath)
    // TODO 2. 处理数据
    val loginLogDataDS = dataDS.map(data => {
      val datas: Array[String] = data.split(",")
      LoginLogData(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
    })
      // TODO 3. 抽取事件时间，并且设置watermark
      .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LoginLogData](Time.seconds(10)) {
        override def extractTimestamp(element: LoginLogData): Long = {
          element.timestamp * 1000
        }
      }
    )

    // TODO 4. 过滤出失败的数据，并且根据用户id进行分组
    val dataKS: KeyedStream[LoginLogData, Long] = loginLogDataDS
      .filter(_.status == "fail")
      .keyBy(_.userId)


    dataKS.process(new KeyedProcessFunction[Long, LoginLogData, String] {


      override def processElement(value: LoginLogData, ctx: KeyedProcessFunction[Long, LoginLogData, String]#Context, out: Collector[String]): Unit = {

      }
    })
  }
}

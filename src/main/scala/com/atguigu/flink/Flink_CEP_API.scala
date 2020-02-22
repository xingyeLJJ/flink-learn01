package com.atguigu.flink

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
  * 如果连续两次水位差超过40cm，发生预警信息
  */
object Flink_CEP_API {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    env.setParallelism(1)

    val dataDS =
      env.readTextFile("input/sensor-data.log")
        //    env.socketTextStream("hadoop102", 4444)
        .map(data => {
        val datas = data.split(",")
        (datas(0), datas(1), datas(2))
      })

    // TODO 声明规则
    val p: Pattern[(String, String, String), (String, String, String)] = Pattern.begin[(String, String, String)]("begin")
      .where(_._1 == "sensor_1")
      // 严格近邻
      //      .next("next")
      // 宽松近邻
      //      .followedBy("followBy")
      .followedByAny("followByAny")
      .where(_._1 == "sensor_2")

    // TODO 应用规则
    val ps: PatternStream[(String, String, String)] = CEP.pattern(dataDS, p)

    // TODO 获取符合规则的数据
    val resultDS: DataStream[String] = ps.select(
      new PatternSelectFunction[(String, String, String), String] {
        override def select(pattern: util.Map[String, util.List[(String, String, String)]]): String = {
          pattern /*.get("begin")*/ .toString
        }
      }
    )
    resultDS.print("pattern>>>>")


    env.execute()
  }
}

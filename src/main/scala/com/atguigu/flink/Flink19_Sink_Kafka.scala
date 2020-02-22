package com.atguigu.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
  * 向kafka写入数据
  */
object Flink19_Sink_Kafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    val dataDS: DataStream[String] = env.readTextFile("input/word.txt")

    dataDS.addSink(new FlinkKafkaProducer011[String]("hadoop104:9092", "waterSensor01", new SimpleStringSchema()))

    env.execute()
  }
}

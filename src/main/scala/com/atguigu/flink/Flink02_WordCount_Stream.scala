package com.atguigu.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
  * 采用flink实现流处理的WordCount
  */
object Flink02_WordCount_Stream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceDS: DataStream[String] = env.socketTextStream("hadoop102", 4444)
    val wordDS = sourceDS.flatMap(_.split(" "))
    val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_, 1))
    // keyBy  [(String, Int), Tuple]  [(String, Int), String]  返回值最后一个类型就是key的类型
    // 如果使用的是索引，会推导不出来，所以最后就会给一个tuple作为key的类型
    // 如果是指明了field，就可以直接推导出类型
    //    val wordToOneKS: KeyedStream[(String, Int), Tuple] = wordToOneDS.keyBy(0)
    val wordToOneKS: KeyedStream[(String, Int), String] = wordToOneDS.keyBy(_._1)

    val wordToSumDS: DataStream[(String, Int)] = wordToOneKS.sum(1)

    wordToSumDS.print()

    env.execute("wordCount")


  }
}

package com.atguigu.util

import org.apache.flink.streaming.api.scala.DataStream

/**
  * 数据访问特质
  */
trait BaseDao {
  def readTextFile(filePath: String): DataStream[String] = {
    FlinkStreamEnv.get().readTextFile(filePath)
  }
}

package com.atguigu.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkStreamEnv {

  private val locals: ThreadLocal[StreamExecutionEnvironment] = new ThreadLocal[StreamExecutionEnvironment]()

  def init(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    locals.set(env)
  }

  def execute(): Unit = {
    locals.get().execute()
  }

  def get(): StreamExecutionEnvironment = {
    locals.get()
  }

}

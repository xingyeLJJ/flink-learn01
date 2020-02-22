package com.atguigu.Function

import com.atguigu.bean.ApacheLogEvent
import org.apache.flink.api.common.functions.AggregateFunction

class HotPageRankAggregateFunction extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

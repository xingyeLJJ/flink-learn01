package com.atguigu.Function

import com.atguigu.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * TODO 热门商品排行聚合函数
  */
class HotItemRankAggregateFunction extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

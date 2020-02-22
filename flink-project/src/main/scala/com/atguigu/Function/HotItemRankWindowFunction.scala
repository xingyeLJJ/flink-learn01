package com.atguigu.Function

import com.atguigu.bean.ItemClickCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class HotItemRankWindowFunction extends WindowFunction[Long, ItemClickCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemClickCount]): Unit = {
    // 聚合后的数据缺失窗口信息，那么无法进行窗口中的排序，所以转换结构，需要增加窗口信息，用于排序操作
    out.collect(ItemClickCount(key, input.iterator.next(), window.getEnd))
  }
}

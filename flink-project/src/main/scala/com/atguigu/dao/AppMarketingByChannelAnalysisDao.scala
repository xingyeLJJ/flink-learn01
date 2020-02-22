package com.atguigu.dao

import com.atguigu.bean.AppMarketData
import com.atguigu.util.{BaseDao, FlinkStreamEnv}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

class AppMarketingByChannelAnalysisDao extends BaseDao {
  def mockData(): DataStream[AppMarketData] = {
    FlinkStreamEnv.get().addSource(
      new SourceFunction[AppMarketData] {

        private var flag = true

        override def run(ctx: SourceFunction.SourceContext[AppMarketData]): Unit = {
          while (flag) {
            ctx.collect(AppMarketData("华为", "INSTALL", System.currentTimeMillis()))
            Thread.sleep(100)
          }
        }

        override def cancel(): Unit = {
          flag = false
        }
      }
    )
  }
}

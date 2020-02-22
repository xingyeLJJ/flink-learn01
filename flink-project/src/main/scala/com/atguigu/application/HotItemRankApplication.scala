package com.atguigu.application

import com.atguigu.common.BaseApplication
import com.atguigu.controller.HotItemRankController

/**
  * 热门商品的排行
  */
object HotItemRankApplication extends App with BaseApplication {
  start {
    val hotItemRankController: HotItemRankController = new HotItemRankController

    hotItemRankController.getHotItemRank(3, "input/UserBehavior.csv").print()
  }


}

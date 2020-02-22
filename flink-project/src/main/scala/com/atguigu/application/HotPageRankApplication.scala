package com.atguigu.application

import com.atguigu.common.BaseApplication
import com.atguigu.controller.HotPageRankController

object HotPageRankApplication extends App with BaseApplication {
  start {
    val hotPageRankController: HotPageRankController = new HotPageRankController()
    hotPageRankController.getHotPageRank(3, "input/apache.log").print()
  }


}

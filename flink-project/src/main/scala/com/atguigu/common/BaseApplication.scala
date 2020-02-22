package com.atguigu.common

import com.atguigu.util.FlinkStreamEnv

trait BaseApplication {
  def start(op: => Unit): Unit = {
    try {
      FlinkStreamEnv.init()
      op
      FlinkStreamEnv.execute()
    } catch {
      case e => e.printStackTrace()
    }
    this
  }

  def close(): Unit = {

  }
}

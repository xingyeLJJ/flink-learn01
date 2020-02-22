package com.atguigu

package object bean {

  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

  // 商品点击数量对象
  case class ItemClickCount(itemId: Long, count: Long, windowEndTime: Long)

  // 日志服务器对象
  case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

  case class UrlViewCount(url: String, windowEnd: Long, count: Long)

  // 应用市场推广数据
  case class AppMarketData(marketing: String, behavior: String, timestamp: Long)

  case class AppMarketCount(marketing: String, behavior: String, count: Long)


  case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

  case class AdClickCount(userId: Long, adId: Long, count: Long)

  case class LoginLogData(userId: Long, ip: String, status: String, timestamp: Long)

  case class OrderLogData(orderId: Long, status: String, txId: String, timestamp: Long)

}

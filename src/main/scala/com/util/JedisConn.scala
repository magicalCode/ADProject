package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConn {

  private val config: JedisPoolConfig = new JedisPoolConfig
  config.setMaxIdle(20)
  config.setMaxTotal(10)



  private val pool = new JedisPool(config,"10.36.151.74",7000)

  def getConn(): Jedis ={
    pool.getResource
  }
}

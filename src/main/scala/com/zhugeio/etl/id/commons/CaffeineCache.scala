package com.zhugeio.etl.id.commons

import com.github.benmanes.caffeine.cache.Caffeine
import com.zhugeio.etl.id.Config

import java.util.concurrent.TimeUnit
import scala.collection.mutable

object CaffeineCache {
  var CAFFINE_CACHE_EXPIRE_MINUTES:Long = Config.getLong(Config.CAFFINE_CACHE_EXPIRE_MINUTES,10)
  var CAFFINE_CACHE_MAX_SIZE:Long = Config.getLong(Config.CAFFINE_CACHE_MAX_SIZE,10000)

  // 初始化设备缓存，设置了 10 分钟的写过期，10000 的缓存最大个数
  private val didCache = Caffeine.newBuilder
   // .expireAfterWrite(10, TimeUnit.MINUTES) //某个数据在多久没有被更新后，就过期。
    .expireAfterAccess(CAFFINE_CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)//最后一次访问之后，隔多久没有被再次访问的话，就过期
   .maximumSize(CAFFINE_CACHE_MAX_SIZE).build[String, String]

  // 初始化设备缓存，设置了 10 分钟的写过期，10000 的缓存最大个数
  private val zgidCache = Caffeine.newBuilder
    // .expireAfterWrite(10, TimeUnit.MINUTES) //某个数据在多久没有被更新后，就过期。
    .expireAfterAccess(CAFFINE_CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)//最后一次访问之后，隔多久没有被再次访问的话，就过期
    .maximumSize(CAFFINE_CACHE_MAX_SIZE).build[String, String]

  def getDeviceMap(deviceIds:mutable.HashSet[String],mapResultGet:collection.mutable.HashMap[String,String]):mutable.HashSet[String]= {
    val notInCahcedeviceIds:mutable.HashSet[String] =  mutable.HashSet[String]()
    if(deviceIds.isEmpty){
      return notInCahcedeviceIds
    }
    deviceIds.foreach(deviceId=>{
      val result = didCache.getIfPresent(deviceId)
        mapResultGet.put(deviceId,result)
      if(result == null){
        notInCahcedeviceIds.add(deviceId)
      }else{
//        println("getDeviceMapFromCache:"+deviceId+":"+result)
      }
    })
    notInCahcedeviceIds
  }

  def getdidCacheSize(): Long = {
    didCache.estimatedSize()
  }

  def putDeviceCache(deviceId:String,zgDeviceId:String): Unit = {
    didCache.put(deviceId,zgDeviceId)
  }

    def main(args: Array[String]): Unit = {
      println("getdidCacheSize",getdidCacheSize())
      val deviceIds = mutable.HashSet[String]()
      deviceIds += "d:1:1"
      deviceIds += "d:1:2"
      deviceIds += "d:1:3"
      deviceIds += "d:1:4"
      putDeviceCache("d:1:1","1")
      println("getdidCacheSize",getdidCacheSize())
      putDeviceCache("d:1:2","2")
      println("getdidCacheSize",getdidCacheSize())
      val mapResultGet = new collection.mutable.HashMap[String,String]()
      val notInCahcedeviceIds =getDeviceMap(deviceIds,mapResultGet)
      println(mapResultGet)
      println(notInCahcedeviceIds)
      putDeviceCache("d:1:3","3")
      putDeviceCache("d:1:4","4")
      val mapResultGet2 = new collection.mutable.HashMap[String,String]()
      val notInCahcedeviceIds2 =getDeviceMap(deviceIds,mapResultGet2)
      println(mapResultGet2)
      println(notInCahcedeviceIds2)
      println("getdidCacheSize",getdidCacheSize())

  }
}

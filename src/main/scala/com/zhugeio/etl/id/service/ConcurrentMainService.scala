package com.zhugeio.etl.id.service

import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors, TimeUnit}

import com.zhugeio.etl.id.ZGMessage

import scala.collection.mutable.ListBuffer;

/**
  * Created by ziwudeng on 12/11/16.
  */
object ConcurrentMainService {

  val limit = 10000
  val singleMod = 10;
  val service = Executors.newFixedThreadPool(singleMod * 5)

  def process(msgs: ListBuffer[ZGMessage], test: Boolean): Unit = {
    if (msgs.size >= limit) {
      val hashs = getHash(msgs)
      val runnables = getCallbles(hashs, test)
      val futures = service.invokeAll(runnables)
      val iterator = futures.iterator()
      while (iterator.hasNext) {
        val future = iterator.next()
        future.get(5, TimeUnit.MINUTES);
      }
    } else {
      MainService.process(msgs)
    }
  }

  def getHash(msgs: ListBuffer[ZGMessage]): util.HashMap[Integer, ListBuffer[ZGMessage]] = {
    val hashMap = new util.HashMap[Integer, ListBuffer[ZGMessage]]()
    var i = 0;
    while (i < singleMod) {
      hashMap.put(i, new ListBuffer[ZGMessage]);
      i = i + 1;
    }

    msgs.foreach(m => {
      hashMap.get(Math.abs(String.valueOf(m.key).hashCode) % singleMod) += m
    })

    hashMap

  }


  def getCallbles(hashs: util.HashMap[Integer, ListBuffer[ZGMessage]], test: Boolean): util.List[Callable[Object]] = {
    val runnables = new util.ArrayList[Callable[Object]]()
    val iterator = hashs.entrySet().iterator()
    while (iterator.hasNext) {
      val list = iterator.next().getValue
      runnables.add(Executors.callable(new Runnable {
        override def run(): Unit = {
          MainService.process(list)
        }
      }));
    }
    runnables
  }
}

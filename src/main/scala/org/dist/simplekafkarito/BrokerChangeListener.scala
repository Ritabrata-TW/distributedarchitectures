package org.dist.simplekafkarito

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging
import org.dist.queue.utils.ZkUtils

import scala.collection.mutable
import scala.collection.mutable.Set

class BrokerChangeListener(zookeeperClient: ZookeeperClient) extends IZkChildListener with Logging {
  //  this.logIdent = "[BrokerChangeListener on Controller " + controller.brokerId + "]: "

  import scala.jdk.CollectionConverters._

  val liveBrokerIds = mutable.Set[Int]()
  val latestAddedBrokers = mutable.Set[ZkUtils.Broker]()

  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
    info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.asScala.mkString(",")))
    try {

      val curBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
      val newBrokerIds = curBrokerIds -- liveBrokerIds
      val newBrokers = newBrokerIds.map(zookeeperClient.getBrokerInfo(_))

      liveBrokerIds ++= newBrokerIds
      latestAddedBrokers ++= newBrokers

      info("%s new brokers added".format(newBrokers.size))

      //      if (newBrokerIds.size > 0)
      //        controller.onBrokerStartup(newBrokerIds.toSeq)

    } catch {
      case e: Throwable => error("Error while handling broker changes", e)
    }
  }
}

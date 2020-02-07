package org.dist.simplekafkarito

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.ZookeeperTestHarness
import org.dist.queue.utils.ZkUtils.Broker

class TestZookeeperClient(brokerIds: List[Int]) extends ZookeeperClient {
  var topicName: String = null
  var partitionReplicas = Set[PartitionReplicas]()
  var topicChangeListner: IZkChildListener = null

  override def registerSelf(): Unit = ???

  override def getAllBrokerIds(): Set[Int] = Set(0, 1, 2)

  override def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = ???

  override def getBrokerInfo(brokerId: Int): Broker = ???

  override def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]): Unit = {
    this.topicName = topicName
    this.partitionReplicas = partitionReplicas
  }
}

class CreateTopicCommandTest extends ZookeeperTestHarness {
  test("should assign set of replicas for partitions of topic") {
    val brokerIds = List(0, 1, 2)

    val zookeeperClient = new TestZookeeperClient(brokerIds)

    val createCommandTest = new CreateTopicCommand(zookeeperClient)
    val noOfPartitions = 3
    val replicationFactor = 2
    createCommandTest.createTopic("topic1", noOfPartitions, replicationFactor)
    assert(zookeeperClient.topicName == "topic1")
    assert(zookeeperClient.partitionReplicas.size == noOfPartitions)
    zookeeperClient.partitionReplicas.map(p => p.brokerIds).foreach(_.size == replicationFactor)
  }
}

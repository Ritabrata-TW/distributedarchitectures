package org.dist.simplekafkarito

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

class ZookeeperClientTest extends ZookeeperTestHarness {
  test("should register broker with zookeeper cluster") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))

    val zookeeperClient: ZookeeperClientRitoImpl = new ZookeeperClientRitoImpl(config1)
    zookeeperClient.registerBroker(Broker(config1.brokerId, config1.hostName, config1.port))

    val config2 = Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    zookeeperClient.registerBroker(Broker(config2.brokerId, config1.hostName, config1.port))

    val brokerIds = zookeeperClient.getAllBrokerIds()
    assert(brokerIds == Set(1, 2))
  }
}

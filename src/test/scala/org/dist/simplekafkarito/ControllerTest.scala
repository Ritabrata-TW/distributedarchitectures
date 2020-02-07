package org.dist.simplekafkarito

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.SimpleSocketServer
import org.dist.util.Networks
import org.mockito.{ArgumentMatchers, Mockito}

class ControllerTest extends ZookeeperTestHarness {
  val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))

  test("should register for broker changes") {
    val zookeeperClient: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    val socketServer = Mockito.mock(classOf[SimpleSocketServer])
    val controller = new Controller(zookeeperClient, config.brokerId, socketServer)
    Mockito.when(zookeeperClient.subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)
    Mockito.when(zookeeperClient.subscribeBrokerChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)
    Mockito.when(zookeeperClient.getAllBrokers()).thenReturn(Set(Broker(1, "10.10.10.10", 9000)))

    controller.startup()

    Mockito.verify(zookeeperClient, Mockito.atLeastOnce()).subscribeBrokerChangeListener(ArgumentMatchers.any[IZkChildListener]())
  }

  test("Should elect first server as controller and register for topic changes") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    val socketServer = Mockito.mock(classOf[SimpleSocketServer])
    val controller = new Controller(zookeeperClient, config.brokerId, socketServer)

    Mockito.when(zookeeperClient.subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)
    Mockito.when(zookeeperClient.subscribeBrokerChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)

    Mockito.when(zookeeperClient.getAllBrokers()).thenReturn(Set(Broker(1, "10.10.10.10", 9000)))

    controller.startup()

    Mockito.verify(zookeeperClient, Mockito.atLeastOnce()).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
  }

  test("Should not register for topic changes if controller already exists") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient1: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])

    val socketServer = Mockito.mock(classOf[SimpleSocketServer])
    val controller1 = new Controller(zookeeperClient1, config.brokerId, socketServer)

    Mockito.doNothing().when(zookeeperClient1).tryCreatingControllerPath("1")
    Mockito.when(zookeeperClient1.getAllBrokers()).thenReturn(Set(Broker(1, "10.10.10.10", 9000)))

    controller1.startup()


    val zookeeperClient2: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    Mockito.doThrow(new ControllerExistsException("1")).when(zookeeperClient2).tryCreatingControllerPath("1")

    val controller2 = new Controller(zookeeperClient2, config.brokerId, socketServer)
    controller2.startup()

    Mockito.verify(zookeeperClient1, Mockito.atLeastOnce()).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
    Mockito.verify(zookeeperClient1, Mockito.atLeastOnce()).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())

    Mockito.verify(zookeeperClient2, Mockito.atMost(0)).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
    Mockito.verify(zookeeperClient2, Mockito.atMost(0)).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
  }

  test("Should select only one server as controller") {
    val zookeeperClient = new ZookeeperClientRitoImpl(config)
    val broker1 = Broker(0, "10.10.10.10", 8000)
    zookeeperClient.registerBroker(broker1)
    val broker2 = Broker(1, "10.10.10.11", 8001)
    zookeeperClient.registerBroker(broker2)
    val broker3 = Broker(2, "10.10.10.12", 8002)
    zookeeperClient.registerBroker(broker3)

    val socketServer = Mockito.mock(classOf[SimpleSocketServer])

    val controller1 = new Controller(zookeeperClient, config.brokerId, socketServer)
    val controller2 = new Controller(zookeeperClient, config.brokerId, socketServer)
    val controller3 = new Controller(zookeeperClient, config.brokerId, socketServer)

    controller1.startup()
    controller2.startup()
    controller3.startup()

    TestUtils.waitUntilTrue(() => {
      controller1.currentLeader == 1 && controller2.currentLeader == 1 && controller3.currentLeader == 1
    }, "Waiting for leader election", 1000)
  }
}

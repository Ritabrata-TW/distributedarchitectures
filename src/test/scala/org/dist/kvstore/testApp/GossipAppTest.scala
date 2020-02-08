package org.dist.kvstore.testApp

import org.dist.kvstore.client.Client
import org.dist.kvstore.testapp.Utils.createDbDir
import org.dist.kvstore.{DatabaseConfiguration, InetAddressAndPort, StorageService}
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

class GossipAppTest extends FunSuite {
  test("should store entry in node of cluster") {
    val localIpAddress = new Networks().ipv4Address

    val node1Endpoint = InetAddressAndPort(localIpAddress, 8000)
    val node1ClientEndpoint = InetAddressAndPort(localIpAddress, 9000)
    val node1 = new StorageService(node1ClientEndpoint, node1Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node1")))

    val node2Endpoint = InetAddressAndPort(localIpAddress, 8001)
    val node2ClientEndpoint = InetAddressAndPort(localIpAddress, 9001)
    val node2 = new StorageService(node2ClientEndpoint, node2Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node2")))

    val node3Endpoint = InetAddressAndPort(localIpAddress, 8002)
    val node3ClientEndpoint = InetAddressAndPort(localIpAddress, 9003)
    val node3 = new StorageService(node3ClientEndpoint, node3Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node3")))

    val node4Endpoint = InetAddressAndPort(localIpAddress, 8003)
    val node4ClientEndpoint = InetAddressAndPort(localIpAddress, 9004)
    val node4 = new StorageService(node4ClientEndpoint, node4Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node3")))

    node1.start()
    node2.start()
    node3.start()
    node4.start()

    TestUtils.waitUntilTrue(() =>
      (node1.tokenMetadata.cloneTokenEndPointMap.size() == 4 &&
        node2.tokenMetadata.cloneTokenEndPointMap.size() == 4 &&
        node3.tokenMetadata.cloneTokenEndPointMap.size() == 4 &&
        node4.tokenMetadata.cloneTokenEndPointMap.size() == 4)
      , "Something happened dude! ", 5000)

    val client = new Client(node1ClientEndpoint)
    val mutationResponses = client.put("table1", "key1", "value1")

    assert(node1.tables.size() + node2.tables.size() + node3.tables.size() + node4.tables.size() == 2)
  }
}

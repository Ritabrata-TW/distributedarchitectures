package org.dist.simplekafkarito

import org.dist.queue.utils.ZkUtils.Broker

case class UpdateMetadataRequest(aliveBrokers:List[Broker], leaderReplicas:List[LeaderAndReplicas])

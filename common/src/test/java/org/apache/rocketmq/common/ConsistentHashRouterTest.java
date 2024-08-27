package org.apache.rocketmq.common;

import org.apache.rocketmq.common.consistenthash.ConsistentHashRouter;
import org.apache.rocketmq.common.consistenthash.Node;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class ConsistentHashRouterTest {

    final int virtualNodeCnt = 10;

    @Test
    public void testRouteNode() {
        Collection<ClientNode> cidNodes = new ArrayList<>();
        //cidNodes.add(new ClientNode("155762#69266371992577"));
        cidNodes.add(new ClientNode("1#1"));
        cidNodes.add(new ClientNode("2#2"));
        cidNodes.add(new ClientNode("3#3"));
        cidNodes.add(new ClientNode("4#4"));

        ConsistentHashRouter<ClientNode> router = new ConsistentHashRouter<>(cidNodes, virtualNodeCnt);
        ClientNode clientNode = router.routeNode("MessageQueue [...]");
    }

    private static class ClientNode implements Node {
        private final String clientID;

        public ClientNode(String clientID) {
            this.clientID = clientID;
        }

        @Override
        public String getKey() {
            return clientID;
        }
    }
}

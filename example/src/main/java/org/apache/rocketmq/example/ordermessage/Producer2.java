/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.ordermessage;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

public class Producer2 {

    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";

    public static void main(String[] args) throws MQClientException {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("ordered_messages_p_group");
            producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
            producer.setDefaultTopicQueueNums(1);
            producer.start();

            String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
            for (int i = 0; i < 100; i++) {
                int orderId = i % 10;
                Message msg =
                    new Message("TopicOrderedMessage", tags[i % tags.length], "KEY" + i,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                //SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                //    @Override
                //    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                //        Integer id = (Integer) arg;
                //        int index = id % mqs.size();
                //        return mqs.get(index);
                //    }
                //}, orderId);
                SendResult sendResult = producer.send(msg);

                System.out.printf("%s%n", sendResult);
            }

            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            throw new MQClientException(e.getMessage(), null);
        }
    }
}

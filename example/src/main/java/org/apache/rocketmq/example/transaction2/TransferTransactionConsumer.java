package org.apache.rocketmq.example.transaction2;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class TransferTransactionConsumer {

    public static final String CONSUMER_GROUP = "transfer_account_cg";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TopicTransfer";

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        pushConsumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        pushConsumer.setConsumeMessageBatchMaxSize(1);
        pushConsumer.subscribe(TOPIC, "*");
        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
                try {
                    for (MessageExt messageExt : msgList) {
                        System.out.println("mock increase B count balance, messageExt=" + messageExt);
                        // 事务消息消费者端和普通消费者无异，消费消息失败并不会回滚生产者的本地事务，
                        // 比如这个转账的案例场景：A 扣款成功后，消费者端接收到事务消息对 B 进行加款操作，但是由于某种原因失败，并不会回滚 A 扣款操作
                        boolean normal = true;  // true 模拟正常，false 模拟失败
                        mockIncreaseB(normal);
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    System.out.printf("consumeMessage error: %s%n", e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });
        pushConsumer.start();
    }

    private static void mockIncreaseB(boolean normal) {
        if (!normal) {
            throw new RuntimeException("mock increase B count balance failed");
        }
    }
}

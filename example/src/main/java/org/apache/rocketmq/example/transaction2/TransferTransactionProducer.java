package org.apache.rocketmq.example.transaction2;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *  结合一个转账的案例说明 RocketMQ 的事务消息
 *  生产者本地事务执行 A 账户扣款，
 *  消费者执行 B 账户加款
 */
public class TransferTransactionProducer {

    public static final String PRODUCER_GROUP = "transfer_account_pg";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TopicTransfer";

    public static void main(String[] args) throws MQClientException, InterruptedException {
        TransactionListener transactionListener = new TransferTransactionListenerImpl();

        // 事务消息生产者
        TransactionMQProducer producer = new TransactionMQProducer(PRODUCER_GROUP, Collections.singletonList(TOPIC));
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000), r -> {
                    Thread thread = new Thread(r);
                    thread.setName("client-transaction-msg-check-thread");
                    return thread;
                });
        producer.setExecutorService(executorService);   // 用于检查本地事务执行状态
        producer.setTransactionListener(transactionListener);
        producer.start();

        // 发送事务消息
        try {
            // 消息内容一般是业务对象序列化的字符串的字节数组
            Message msg = new Message(TOPIC,
                    ("transfer bill serialized content ...").getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.sendMessageInTransaction(msg, null);
            System.out.printf("%s%n", sendResult);
        } catch (MQClientException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }
}

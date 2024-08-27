package org.apache.rocketmq.example.transaction2;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;

public class TransferTransactionListenerImpl implements TransactionListener {

    // 本地事务执行状态，一般存储到专门的事务日志服务或者跟随业务服务存储
    private final ConcurrentHashMap<String, LocalTransactionState> transactionStates = new ConcurrentHashMap<>();

    /**
     * 执行本地事务
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        String transactionId = msg.getTransactionId();
        System.out.println("execute local transaction with ID: " + transactionId);
        try {
            // msg 反序列化 ...
            // 执行本地事务，即带着事务处理（比如被 @Transactional 注释）的业务方法
            mockExecLocalTransaction();
            // 执行成功，提交事务
            transactionStates.put(transactionId, LocalTransactionState.COMMIT_MESSAGE);
            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (Exception e) {
            System.out.println("execute local transaction error: " + e);
            // 出现异常回滚
            transactionStates.put(transactionId, LocalTransactionState.ROLLBACK_MESSAGE);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        LocalTransactionState state = transactionStates.get(msg.getTransactionId());
        if (null != state) {
            return state;
        }
        return LocalTransactionState.UNKNOW;
    }

    private void mockExecLocalTransaction() {
        System.out.println("mock executing local transaction, A count deduct $100");
        //boolean normal = true;
        boolean normal = false;
        // 模拟业务执行中可能出现异常, 本地事务通常会自动回滚
        if (!normal) {
            throw new RuntimeException("some exception occurred");
        }
    }
}

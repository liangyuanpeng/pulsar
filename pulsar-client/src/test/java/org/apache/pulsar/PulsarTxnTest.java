package org.apache.pulsar;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarTxnTest extends PulsarClientTest{

    public PulsarTxnTest(){
        super(true);
    }

    @Test
    public void testTxn() throws PulsarClientException, ExecutionException, InterruptedException {
        int timeout=3;
        Transaction transaction = client.newTransaction().withTransactionTimeout(timeout, TimeUnit.SECONDS).build().get();
        TimeUnit.SECONDS.sleep(5);
        System.out.println(transaction.getTxnID().toString());
        String topic = "persistent://public/default/testtxn";
        Consumer consumer = createConsumer(client, topic);
        CompletableFuture.runAsync(()->{
            System.out.println("===begin receive===");
            while(true){
                try {
                    Message<byte[]> receive = consumer.receive();
                    System.out.println("receive:"+new String(receive.getValue()));
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        Producer<byte[]> producer = createProducer(client, topic);
        MessageId send = producer.newMessage(transaction).value("hello txn".getBytes())
                .eventTime(1)
                .send();
        //TODO ResetDynamicConfigToDefault  test
        System.out.println(send.toString());
//        producer.newMessage(transaction).value("hello txn".getBytes()).send();
//        producer.newMessage(transaction).value("hello txn2".getBytes()).send();
//        Message<byte[]> receive = consumer.receive();
//        System.out.println(new String(receive.getValue()));
        TimeUnit.SECONDS.sleep(5);
        System.out.println("commit");
        transaction.commit().get();
        TimeUnit.SECONDS.sleep(timeout);
//        transaction.abort();
//        client.close();
    }

}

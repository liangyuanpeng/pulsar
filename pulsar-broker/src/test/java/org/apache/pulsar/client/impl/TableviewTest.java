//package org.apache.pulsar.client.impl;
//
//import com.google.protobuf.ListValue;
//import com.google.protobuf.Value;
//import org.apache.commons.lang3.RandomUtils;
//import org.apache.pulsar.client.api.*;
//import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
//
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeUnit;
//import java.util.stream.Collectors;
//
//public class TableviewTest {
//    public static void main(String[] args) throws PulsarClientException, InterruptedException {
//        String topic = "persistent://public/default/testtt";
//
//        String brokerUrl = "pulsar://smartoilets.cn:26650";
//
//        PulsarClient client = PulsarClient.builder()
//                .serviceUrl(brokerUrl)
//                .enableTlsHostnameVerification(false) // false by default, in any case
//                .allowTlsInsecureConnection(true) // false by default, in any case
//                .build();
//        Producer producer = client.newProducer(Schema.STRING)
//                .producerName("producer4")
//                .autoUpdatePartitions(true)
//                .topic(topic)
//                .create();
//
//        boolean subscribeFlag = true;
//
//        if(subscribeFlag){
//            Consumer<String.class> subscribe = client.newConsumer(Schema.STRING)
//                    .enableBatchIndexAcknowledgment(true)
//                    .autoUpdatePartitions(true)
//                    .topicsPattern(topic)
//                    .subscriptionName("hello23")
//                    .subscriptionTopicsMode(RegexSubscriptionMode.AllTopics)
//                    .consumerName("hello")
//                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
//
//                    .subscribe();
//
//            //RegexSubscriptionMode
//            CompletableFuture.runAsync(()->{
//                while (true){
//                    try {
//                        Message<String> receive = subscribe.receive(1, TimeUnit.SECONDS);
//                        if(receive!=null){
//                            System.out.println("");
//                        }
//                    } catch (PulsarClientException e) {
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }
//        TimeUnit.SECONDS.sleep(3);
//
//        boolean produce = false;
//        if(produce){
//            while(true){
//                TimeUnit.SECONDS.sleep(2);
//                if(producer.isConnected()){
//                    System.out.println("producer.send.message:"+producer.getTopic()+"|"+producer.getProducerName());
//                    TypedMessageBuilder typedMessageBuilder = producer.newMessage();
//                    String data = String.valueOf(RandomUtils.nextInt(10,100));
//
//                    typedMessageBuilder = typedMessageBuilder
//                            .property("siteName","aeon")
//                            .property("wcName","gt3")
//                            .property("room","male")
//                            .eventTime(System.currentTimeMillis())
//                            .key(data)
//                            .value("world");
//                    typedMessageBuilder.send();
//                }else{
//                    System.out.println("produce.is.not .connect!"+producer.getProducerName());
//                }
//            }
//        }
//
//
//    }
//
//}

package org.apache.pulsar;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarClientTest {

    boolean txn;

    public PulsarClientTest(boolean txn){
        this.txn=txn;
    }

    public static Consumer createConsumer(PulsarClient client, String topic) throws PulsarClientException {
        Consumer<byte[]> subscribe = client.newConsumer()
                .consumerName("test")
                .subscriptionName("test")
                .topic(topic)
                .subscribe();
        return subscribe;
    }

    public static Producer createProducer(PulsarClient client, String topic) throws PulsarClientException {
        return client.newProducer()
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(topic)
                .producerName("test")
                .create();
    }

    PulsarClient client;

    @Before
    public void setup() throws PulsarClientException {
        client = PulsarClient.builder()
                .enableTransaction(txn)
//                .statsInterval()
//                .tlsTrustCertsFilePath("/usr/local/smartoilets/pulsar/ca/ca.cert.pem")
//                .tlsTrustCertsFilePath("/home/yunhorn/server/apache-pulsar-2.8.0/myca/certs/ca.cert.pem")
//                .tlsTrustCertsFilePath("/home/yunhorn/server/apache-pulsar-2.8.1/my-ca/certs/ca.cert.pem")
//                .tlsTrustCertsFilePath("/tmp/ca.cert.pem")
//                .enableTls(true)
                .serviceUrl("pulsar://192.168.31.90:6650")
                .enableTlsHostnameVerification(false) // false by default, in any case
                .allowTlsInsecureConnection(true) // false by default, in any case
                .build();
    }

    @After
    public void close() throws PulsarClientException {
        if(client!=null){
            client.close();
        }
    }
}

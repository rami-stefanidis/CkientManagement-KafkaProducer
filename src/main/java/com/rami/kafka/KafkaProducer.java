package com.rami.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * Created by Rami Stefanidis on 6/25/2017.
 */
@Component
public class KafkaProducer {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    public void send(final String topic,final String message) {
        LOG.info("Begin send. topic = {} , message = {} ",topic, message);

        final ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
        // register a callback with the listener to receive the result of the send asynchronously
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOG.info("sent message='{}' with offset={}", message,
                        result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                LOG.error("unable to send message='{}'", message, ex);
            }
        });
    }
}

package com.example.producer;

import org.apache.kafka.common.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import com.example.dto.MessageDTO;

@Component
public class KafkaMessageProducer {
	
	private static Logger LOGGER = LoggerFactory.getLogger(KafkaMessageProducer.class);
	
	private static final String TOPIC = "thread-topic";
	
	@Autowired
	@Qualifier("producer")
	private KafkaTemplate<String, String> kafkaTemplate;


	public void sendMessage(MessageDTO messageDTO) {
		
		 org.springframework.messaging.Message<MessageDTO> message = MessageBuilder
	                .withPayload(messageDTO)
	                .setHeader(KafkaHeaders.TOPIC, TOPIC)
	                .build();
		ListenableFuture<SendResult<String,String>> future = kafkaTemplate.send(message);

        future.addCallback(new KafkaSendCallback<String, String>() {

            @Override
            public void onFailure(KafkaProducerException ex) {
            	LOGGER.warn("Message could not be delivered. " + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
            	LOGGER.info("Your message was delivered with following offset: " + result.getRecordMetadata().offset());
            }
        });
	}
}
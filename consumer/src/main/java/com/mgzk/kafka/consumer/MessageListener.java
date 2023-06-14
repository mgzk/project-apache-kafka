package com.mgzk.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MessageListener {

  private static final String TOPIC = "events";

  @KafkaListener(topics = TOPIC, groupId = "default")
  public void listen(@Payload String message,
                     @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
    log.info(String.format("Receive message: %s from partition: %d", message, partition));
  }
}

package com.mgzk.kafka.producer;

import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessagePublisher {

  private static final String TOPIC = "events";

  private final KafkaTemplate<String, String> kafkaTemplate;

  @Scheduled(fixedRate = 1000)
  public void publish() {
    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, UUID.randomUUID().toString());
    kafkaTemplate.send(record)
      .whenComplete((result, ex) -> {
      if (result != null) {
        log.info(String.format("Send message: %s in topic: %s", result.getProducerRecord().value(), result.getProducerRecord().topic()));
      }
    });
  }
}

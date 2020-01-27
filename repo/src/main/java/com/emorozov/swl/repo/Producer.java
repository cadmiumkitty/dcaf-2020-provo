package com.emorozov.swl.repo;

import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import lombok.extern.java.Log;

import java.time.LocalDateTime;

@Component
@Log
public class Producer {

  @Value("repo.topic.repo")
  private String repoTopic;

  @Value("repo.topic.prov")
  private String provTopic;

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Scheduled(fixedRate = 5000)
  public void sendMessage() {
    String message = LocalDateTime.now().toString();
    log.info(message);
    this.kafkaTemplate.send(repoTopic, message);
    this.kafkaTemplate.send(provTopic, message);
  }
}
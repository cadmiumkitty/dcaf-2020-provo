package com.emorozov.swl.repo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableAutoConfiguration
@EnableKafka
@EnableKafkaStreams
@EnableScheduling
public class Application {

  public static void main(String[] args) throws Exception {
    Thread.sleep(15000L);
    SpringApplication.run(Application.class, args);
  }
}
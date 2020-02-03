package com.emorozov.swl.repo;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class RiskCalculatorConfiguration {

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kStreamsConfigs() {
    final Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "riskCalculator");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
    return new KafkaStreamsConfiguration(props);
  }

  @Bean
  public StreamsBuilderFactoryBeanCustomizer customizer() {
    return fb -> fb.setStateListener((newState, oldState) -> {
      log.info("State transition from {} to {}", oldState, newState);
    });
  }

  @Bean
  public KStream<String, String> counterpartyKStream(StreamsBuilder kStreamBuilder) {
    KStream<String, String> counterparties = kStreamBuilder.stream("counterparties");
    return counterparties;
  }

  @Bean
  public KStream<String, String> trades(StreamsBuilder kStreamBuilder, KStream<String, String> counterparties) {
    KStream<String, String> trades = kStreamBuilder.stream("trades");
    trades.outerJoin(counterparties,
        (trade, counterparty) -> "<http://semanticweblondon.com/risk> a <http://www.w3.org/ns/prov#Entity> .",
        JoinWindows.of(Duration.ofSeconds(60))).to("prov");
    return trades;
  }
}
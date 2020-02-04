package com.emorozov.swl.repo;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class RiskCalculatorConfiguration {

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kStreamsConfigs() {

    // Spring does not support all of the propersites, for example
    // DEFAULT_KEY_SERDE_CLASS_CONFIG, DEFAULT_VALUE_SERDE_CLASS_CONFIG,
    // DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG
    // in the config file so we have to set it up manually here
    final Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "risk");
    props.put(StreamsConfig.POLL_MS_CONFIG, 100);
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
    props.put(StreamsConfig.producerPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 1);
    props.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "1");

    return new KafkaStreamsConfiguration(props);
  }

  @Bean
  public StreamsBuilderFactoryBeanCustomizer customizer() {
    return fb -> fb.setStateListener((newState, oldState) -> {
      log.info("State transition from {} to {}", oldState, newState);
    });
  }

  @Bean
  public KStream<String, String> provKStream(StreamsBuilder kStreamBuilder, RiskCalculator riskCalculator) {

    // In this simple app just join two KTable to simulate calculation of
    // counterparty risk. Ignore more complex topology required to emit two
    // different messages on different topics, and just record the provenance so
    // that we can visualize it.
    // Will also need to deal with the case where either trade or counterparty does
    // not yet exist for the counterparty
    KTable<String, String> counterparties = kStreamBuilder.table("counterparties");
    KTable<String, String> trades = kStreamBuilder.table("trades");
    KStream<String, String> prov = trades.outerJoin(counterparties,
        (trade, counterparty) -> riskCalculator.calculateRiskAndRecordProvenance(trade, counterparty))
        .filterNot((k, v) -> v.startsWith("ERROR")).toStream();
    prov.to("prov");
    return prov;
  }
}
package com.emorozov.swl.repo;

import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;
import org.openprovenance.prov.interop.Formats;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Activity;
import org.openprovenance.prov.model.Agent;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.Entity;
import org.openprovenance.prov.model.ProvFactory;
import org.openprovenance.prov.model.Namespace;
import org.openprovenance.prov.model.QualifiedName;
import org.openprovenance.prov.model.SpecializationOf;
import org.openprovenance.prov.model.WasAssociatedWith;
import org.openprovenance.prov.model.WasDerivedFrom;
import org.openprovenance.prov.model.WasStartedBy;
import org.openprovenance.prov.model.WasEndedBy;
import org.openprovenance.prov.model.WasGeneratedBy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.UUID;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

@Component
@Slf4j
public class TradeEventProcessor {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private Namespace ns;

  @Autowired
  private ProvFactory provFactory;

  @Autowired
  private InteropFramework interopFramework;

  @Value("${repo.topic.trades-topic}")
  private String tradesTopic;

  @Value("${repo.topic.prov-topic}")
  private String provTopic;

  private String tradeId = UUID.randomUUID().toString();

  private String counterpartyId = "cpty-1";

  private int tradeVersionCounter = 5;

  @Scheduled(fixedRate = 30000)
  public void sendMessage() {

    String eventId = UUID.randomUUID().toString();
    OffsetDateTime odt = OffsetDateTime.now();

    int oldTradeVersionNumber = tradeVersionCounter;
    int newTradeVersionNumber = ++tradeVersionCounter;

    String tradeMessage = createTradeFromTradeEvent(tradeId, oldTradeVersionNumber, newTradeVersionNumber, eventId, odt);
    String provMessage = createProvFromTradeEvent(tradeId, oldTradeVersionNumber, newTradeVersionNumber, eventId,
        odt);

    this.kafkaTemplate.send(tradesTopic, counterpartyId, tradeMessage);
    this.kafkaTemplate.send(provTopic, provMessage);
  }

  private String createTradeFromTradeEvent(String tradeId, int oldTradeVersionNumber, int newTradeVersionNumber,
      String eventId, OffsetDateTime odt) {
    return String.format("Correction %s for trade %s at %s (version %s -> %s)", tradeId,
        eventId, odt.toString(), oldTradeVersionNumber, newTradeVersionNumber);
  }

  @SneakyThrows
  private String createProvFromTradeEvent(String tradeId, int oldTradeVersionNumber, int newTradeVersionNumber,
      String eventId, OffsetDateTime odt) {

    QualifiedName tradeQn = qn(String.format("trade-%s", tradeId));
    QualifiedName oldTradeVersionQn = qn(String.format("trade-%s-%s", tradeId, oldTradeVersionNumber));
    QualifiedName newTradeVersionQn = qn(String.format("trade-%s-%s", tradeId, newTradeVersionNumber));
    QualifiedName tradeCorrectionQn = qn(String.format("activity-%s", eventId));
    QualifiedName traderQn = qn("johnsmith");
    QualifiedName tradeEventProcessorQn = qn("trade-event-processor");

    Entity trade = provFactory.newEntity(tradeQn, "Term Repo.");
    Entity newTradeVersion = provFactory.newEntity(newTradeVersionQn,
        String.format("Term Repo version %s", newTradeVersionNumber));
    SpecializationOf newTradeVersionSpecializationOf = provFactory.newSpecializationOf(newTradeVersionQn, tradeQn);

    GregorianCalendar gcStartTime = GregorianCalendar.from(odt.atZoneSameInstant(ZoneId.of("Z")));
    XMLGregorianCalendar xmlgcStartTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gcStartTime);
    GregorianCalendar gcEndTime = GregorianCalendar.from(odt.atZoneSameInstant(ZoneId.of("Z")));
    XMLGregorianCalendar xmlgcEndTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gcEndTime);

    Activity tradeCorrection = provFactory.newActivity(tradeCorrectionQn, xmlgcStartTime, xmlgcEndTime,
        Collections.emptyList());
    provFactory.addLabel(tradeCorrection, String.format("Trade correction on %s", odt.toString()));

    WasGeneratedBy newTradeVersionWasGeneratedBy = provFactory.newWasGeneratedBy(null, newTradeVersionQn,
        tradeCorrectionQn);

    Agent trader = provFactory.newAgent(traderQn, "John Smith");
    WasStartedBy wasStartedBy = provFactory.newWasStartedBy(null, tradeCorrectionQn, traderQn);
    WasEndedBy wasEndedBy = provFactory.newWasEndedBy(null, tradeCorrectionQn, traderQn);

    Agent tradeEventProcessor = provFactory.newAgent(tradeEventProcessorQn, "Trade Event Processor");
    WasAssociatedWith wasAssociatedWith = provFactory.newWasAssociatedWith(null, tradeCorrectionQn,
        tradeEventProcessorQn);

    WasDerivedFrom tradeVersionDerivation = provFactory.newWasDerivedFrom(null, newTradeVersionQn, oldTradeVersionQn);

    Document repoEventProvDocument = provFactory.newDocument();
    repoEventProvDocument.getStatementOrBundle()
        .addAll(Arrays.asList(trade, newTradeVersion, newTradeVersionSpecializationOf, tradeCorrection, trader,
            tradeEventProcessor, wasStartedBy, wasEndedBy, wasAssociatedWith, tradeVersionDerivation,
            newTradeVersionWasGeneratedBy));
    repoEventProvDocument.setNamespace(ns);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    interopFramework.writeDocument(baos, Formats.ProvFormat.TURTLE, repoEventProvDocument);

    return baos.toString();
  }

  public QualifiedName qn(String name) {
    return ns.qualifiedName(EventProcessorConfiguration.SWL_PREFIX, name, provFactory);
  }
}
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
import org.springframework.kafka.core.KafkaTemplate;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.UUID;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

@Component
@Slf4j
public class TradeEventProcessor {

  private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:SS");

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private Namespace ns;

  @Autowired
  private ProvFactory provFactory;

  @Autowired
  private InteropFramework interopFramework;

  private String tradeId = UUID.randomUUID().toString();

  private int tradeVersionCounter = 0;

  @Scheduled(fixedDelay = 30000, initialDelay = 30000)
  public void sendMessage() {

    String counterpartyId = "bank-x";
    String eventId = UUID.randomUUID().toString();
    OffsetDateTime odt = OffsetDateTime.now();

    int oldTradeVersionNumber = tradeVersionCounter;
    int newTradeVersionNumber = ++tradeVersionCounter;

    String tradeMessage = createTradeMessage(tradeId, oldTradeVersionNumber, newTradeVersionNumber, eventId, odt);
    String provMessage = createProvMessage(tradeId, oldTradeVersionNumber, newTradeVersionNumber, eventId, odt);

    // Ignores transactions in this simple example
    this.kafkaTemplate.send("trades", counterpartyId, tradeMessage);
    this.kafkaTemplate.send("prov", provMessage);
  }

  private String createTradeMessage(String tradeId, int oldTradeVersionNumber, int newTradeVersionNumber,
      String eventId, OffsetDateTime odt) {

    log.info("Correction {} for trade {} at {} (version {} -> {})", eventId, tradeId, odt.format(DEFAULT_FORMATTER),
        oldTradeVersionNumber, newTradeVersionNumber);
    return String.format("%s-%s", tradeId, newTradeVersionNumber);
  }

  @SneakyThrows
  private String createProvMessage(String tradeId, int oldTradeVersionNumber, int newTradeVersionNumber, String eventId,
      OffsetDateTime odt) {

    QualifiedName tradeQn = qn(String.format("trade-%s", tradeId));
    QualifiedName oldTradeVersionQn = qn(String.format("trade-%s-%s", tradeId, oldTradeVersionNumber));
    QualifiedName newTradeVersionQn = qn(String.format("trade-%s-%s", tradeId, newTradeVersionNumber));
    QualifiedName tradeCorrectionQn = qn(String.format("activity-%s", eventId));
    QualifiedName traderQn = qn("johnsmith");
    QualifiedName tradeEventProcessorQn = qn("trade-event-processor");

    Entity trade = provFactory.newEntity(tradeQn, "RR 10M FNMA 7.125 01-15-30");
    Entity newTradeVersion = provFactory.newEntity(newTradeVersionQn,
        String.format("RR 10M FNMA 7.125 01-15-30 version %s", newTradeVersionNumber));
    SpecializationOf newTradeVersionSpecializationOf = provFactory.newSpecializationOf(newTradeVersionQn, tradeQn);

    GregorianCalendar gcStartTime = GregorianCalendar.from(odt.atZoneSameInstant(ZoneId.of("Z")));
    XMLGregorianCalendar xmlgcStartTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gcStartTime);
    GregorianCalendar gcEndTime = GregorianCalendar.from(odt.atZoneSameInstant(ZoneId.of("Z")));
    XMLGregorianCalendar xmlgcEndTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gcEndTime);

    Activity tradeCorrection = provFactory.newActivity(tradeCorrectionQn, xmlgcStartTime, xmlgcEndTime,
        Collections.emptyList());
    provFactory.addLabel(tradeCorrection, String.format("Trade correction on %s", odt.format(DEFAULT_FORMATTER)));

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
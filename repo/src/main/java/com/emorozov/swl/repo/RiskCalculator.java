package com.emorozov.swl.repo;

import java.io.ByteArrayOutputStream;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.openprovenance.prov.interop.Formats;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Activity;
import org.openprovenance.prov.model.Agent;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.Entity;
import org.openprovenance.prov.model.Namespace;
import org.openprovenance.prov.model.ProvFactory;
import org.openprovenance.prov.model.QualifiedName;
import org.openprovenance.prov.model.Used;
import org.openprovenance.prov.model.WasDerivedFrom;
import org.openprovenance.prov.model.WasEndedBy;
import org.openprovenance.prov.model.WasGeneratedBy;
import org.openprovenance.prov.model.WasStartedBy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RiskCalculator {

  private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:SS");

  @Autowired
  private Namespace ns;

  @Autowired
  private ProvFactory provFactory;

  @Autowired
  private InteropFramework interopFramework;

  @SneakyThrows
  public String calculateRiskAndRecordProvenance(String trade, String counterparty) {

    if (trade == null || counterparty == null) {
      return String.format("ERROR: Could not process update for trade %s and counterparty %s", trade, counterparty);
    }

    OffsetDateTime odt = OffsetDateTime.now();

    log.info("Calculating risk for trade {} counterparty {} at {}", trade, counterparty, odt);

    QualifiedName riskQn = qn(String.format("risk-%s-%s", trade, counterparty));
    QualifiedName tradeVersionQn = qn(String.format("trade-%s", trade));
    QualifiedName counterpartyVersionQn = qn(String.format("cpty-%s", counterparty));
    QualifiedName riskCalculationQn = qn(String.format("risk-calculation-%s-%s", trade, counterparty));
    QualifiedName riskCalculatorQn = qn("risk-calculator-1");

    Entity risk = provFactory.newEntity(riskQn,
        String.format("Risk for trade %s counterparty %s at %s", trade, counterparty, odt.format(DEFAULT_FORMATTER)));

    GregorianCalendar gcStartTime = GregorianCalendar.from(odt.atZoneSameInstant(ZoneId.of("Z")));
    XMLGregorianCalendar xmlgcStartTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gcStartTime);
    GregorianCalendar gcEndTime = GregorianCalendar.from(odt.atZoneSameInstant(ZoneId.of("Z")));
    XMLGregorianCalendar xmlgcEndTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gcEndTime);

    Activity riskCalculation = provFactory.newActivity(riskCalculationQn, xmlgcStartTime, xmlgcEndTime,
        Collections.emptyList());
    provFactory.addLabel(riskCalculation, String.format("Risk calculation at %s", odt.format(DEFAULT_FORMATTER)));

    Used riskCalculationUsedTrade = provFactory.newUsed(null, riskCalculationQn, tradeVersionQn);
    Used riskCalculationUsedCounterparty = provFactory.newUsed(null, riskCalculationQn, counterpartyVersionQn);

    WasGeneratedBy newRiskWasGeneratedBy = provFactory.newWasGeneratedBy(null, riskQn, riskCalculationQn);

    Agent riskCalculator = provFactory.newAgent(riskCalculatorQn, "Risk calculator");
    WasStartedBy wasStartedBy = provFactory.newWasStartedBy(null, riskCalculationQn, riskCalculatorQn);
    WasEndedBy wasEndedBy = provFactory.newWasEndedBy(null, riskCalculationQn, riskCalculatorQn);

    WasDerivedFrom tradeVersionDerivationFromTrade = provFactory.newWasDerivedFrom(null, riskQn, tradeVersionQn);
    WasDerivedFrom tradeVersionDerivationFromCounterparty = provFactory.newWasDerivedFrom(null, riskQn,
        counterpartyVersionQn);

    Document repoEventProvDocument = provFactory.newDocument();
    repoEventProvDocument.getStatementOrBundle()
        .addAll(Arrays.asList(risk, riskCalculation, riskCalculationUsedTrade, riskCalculationUsedCounterparty,
            newRiskWasGeneratedBy, riskCalculator, wasStartedBy, wasEndedBy, tradeVersionDerivationFromTrade,
            tradeVersionDerivationFromCounterparty));
    repoEventProvDocument.setNamespace(ns);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    interopFramework.writeDocument(baos, Formats.ProvFormat.TURTLE, repoEventProvDocument);

    return baos.toString();
  }

  public QualifiedName qn(String name) {
    return ns.qualifiedName(EventProcessorConfiguration.SWL_PREFIX, name, provFactory);
  }
}
# Data Provenance and PROV-O Ontology talk at DCAF 2020

## Introduction

The purpose of this demo is to show capturing of the provenance information using common vocabulary of PROV in a repo trading and risk reporting scenario. It is built using Event Sourcing and CQRS patterns on top of Kafka.

## Set up

 1. Single Kafka node with single Zookeeper node
 1. Repo producer that creates and amends repo trades based on trade events
 1. Counterparty producer that creates and amends counterparty records
 1. Risk calculator that calculates risk figures based on repo events
 1. Provenance aggregator as a Kafka Connect node
 1. Simple Jena Fuseki triplestore to aggregate PROV data
 1. Prov-O-Viz set up for simple visualization

## Running the demo

Build individual projects under `repo` (trade and counterparty events, risk calculator and event processor) and `connect` (Kafka Connect SPARQL sink for PROV) with `mvn clean package`.

Build and start containers with:

```
docker-compose up -d --build
```

Once containers are up and running, you can check that PROV triples are being created in Jena by going to http://localhost:3030/dataset.html?tab=query&ds=/dcaf and issuing simple SPARQL query such as:

```
SELECT *
WHERE {
  ?s ?p ?o
}
```

To view visualization go to http://localhost:5000/ and select endpoint http://fuseki:3030/dcaf/query telling PROV-O-Viz to `Ignore Named Graphs`.

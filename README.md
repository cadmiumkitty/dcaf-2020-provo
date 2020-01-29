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

```
docker-compose up -d --build
```

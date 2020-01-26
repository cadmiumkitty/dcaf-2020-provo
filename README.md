# Data Provenance and PROV-O Ontology talk at DCAF

## Introduction

The purpose of this demo is to show capturing of the provenance information using common vocabulary of PROV in a repo trading and risk reporting scenario. It is built using Event Sourcing and CQRS patterns on top of Kafka.

## Set up

 1. Simplest Kafka set up with single Zookeeper node and single Kafka node.
 1. Simple Repo producer that creates and amends repo trades
 1. Simple Risk calculator that calculates risk figures based on repo events
 1. Simple provenance aggregator exposing SPARQL endpoint for provenance queries

## Running the demo

```
docker-compose up -d --build
```


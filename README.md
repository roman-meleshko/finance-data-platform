# finance-data-platform

A data platform for private-bank positions and market risk. It comes in two halves: a batch pipeline that builds the reference and position data, and a streaming layer that revalues the book against live market prices. Still being built.

## Stack

dbt, BigQuery, Airflow, Spark, Kafka, GitHub Actions.

## Status

Early. Right now it is a dbt-core project running against BigQuery with a single model. Nothing below is built yet.

## Planned

- Batch core: counterparties from GLEIF, instruments from ESMA FIRDS, and a generated position book, modelled as layered dbt with tests
- Airflow to schedule and orchestrate the pipeline
- A Spark Structured Streaming job for the live price feed
- CI that runs the dbt build, and an Azure storage deployment

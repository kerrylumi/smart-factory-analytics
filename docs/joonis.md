```mermaid
flowchart LR
    subgraph Allikad
        SIM[metalfab-simulator<br/>PackML olekud + telemeetria]
        EL[Elering NPS API<br/>börsihind]
    end

    subgraph Sissevõtt
        SIM -->|MQTT| HM[HiveMQ CE]
        HM --> RPC[Redpanda Connect<br/>JSON write]
        EL -->|HTTPS| ING[Airflow DAG<br/>elering_ingest]
    end

    RPC --> LAKE[(data/lake<br/>Hive-partitioned JSON)]
    LAKE --> SPK[Jupyter PySpark<br/>Structured Streaming]
    SPK --> RFD[(bronze.raw_factory_data)]
    ING --> BR[(Bronze<br/>br_electricity_prices)]
    RFD --> SLV[(Silver<br/>puhastatud view'd)]
    BR --> SLV
    SLV --> GLD[(Gold<br/>star-skeem<br/>OEE • Energy • Downtime)]
    GLD --> SUP[Superset Dashboard]

    subgraph Orkestreerimine ja kvaliteet
        AF[Airflow<br/>dbt run + test]
    end
    AF -.orkestreerib.-> BR
    AF -.orkestreerib.-> SLV
    AF -.orkestreerib.-> GLD
```

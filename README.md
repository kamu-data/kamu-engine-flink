# Apache Flink Engine

This the implementation of the `Engine` contract of [Open Data Fabric](http://opendatafabric.org/) using the [Apache Flink](https://flink.apache.org/) stream processing framework. It is currently in use in [kamu-cli](https://github.com/kamu-data/kamu-cli) data management tool.

## Features

Flink engine currently provides the most rich functionality for aggregating and joining event streams.

### Windowed Aggregations
Example:
```sql
SELECT
    TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
    symbol as symbol,
    min(price) as `min`,
    max(price) as `max`
FROM `in`
GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
```

### Stream-To-Stream Joins
Example:
```sql
SELECT
  o.event_time as order_time,
  o.order_id,
  o.quantity as order_quantity,
  CAST(s.event_time as TIMESTAMP) as shipped_time,
  COALESCE(s.num_shipped, 0) as shipped_quantity
FROM
  orders as o
LEFT JOIN shipments as s
ON
  o.order_id = s.order_id
  AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '2' DAY
```

### Temporal Table Joins
Example:
```sql
SELECT
  t.event_time,
  t.symbol,
  owned.volume as volume,
  t.price as current_price,
  owned.volume * t.price as current_value
FROM
  tickers as t,
  LATERAL TABLE (`stocks.owned`(t.event_time)) AS owned
WHERE t.symbol = owned.symbol
```

## Known Issues

- Takes a long time to start up which is hurting the user experience
- SQL parser is very sensitive to keywords and requires a lot of quoting
- DECIMAL data type is broken in Parquet [FLINK-17804](https://issues.apache.org/jira/browse/FLINK-17804)
  - Pull request created [flink#12768](https://github.com/apache/flink/pull/12768) and waiting for approval
  - Using our forked version for now
- Does not save watermarks in savepoints [FLINK-5601](https://issues.apache.org/jira/browse/FLINK-5601)
  - Manually saving watermarks as part of the checkpoint
- Does not support month/quarter/year tumbling windows [FLINK-9740](https://jira.apache.org/jira/browse/FLINK-9740)
- Does not support nested data in Parquet [FLINK-XXX]()
- Does not have "process available inputs and stop with savepoint" execution mode
  - We patch some internal classes in order to flush the data and trigger the savepoint when there is no more data to consume
- Does not (easily) support resuming from savepoint programmatically
  - Have to resume from savepoint using CLI rather than in a program
- Does not support Apache Arrow format [FLINK-10929](https://issues.apache.org/jira/browse/FLINK-10929)
- Requires conversion to Avro to be able to save to Parquet
- Does not support late data handling in SQL API [FLINK-XXX](https://stackoverflow.com/questions/51172965/flink-use-allowedlateness-in-flink-sql-api)
- Does not support temporal table joins without or with compound primary key [FLINK-XXX]()

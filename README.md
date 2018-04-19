# talk-stream-processing-et-sql

Support pour un talk (format quickie) sur le stream processing avec SQL

## Demos

For the demo, first start the log generator
```bash
mvn -Ploggen exec:java
```

### streaming-sql

```bash
./streaming-sql
```

Query examples:
```sql
SELECT STREAM * FROM weblogs WHERE status = '500';
```

```sql
SELECT STREAM 
TUMBLE_END(rowtime, INTERVAL '10' SECOND),
url,
COUNT(*) AS nb_requests,
SUM(nb_bytes) AS total_bytes  
FROM weblogs 
GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), url;
```

```sql
SELECT STREAM
HOP_END(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) AS rowtime,
COUNT(*) AS nb_requests,
SUM(nb_bytes) AS total_bytes
FROM weblogs
GROUP BY HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND);
```


### sqlline

```bash
./sqlline
sqlline> !connect jdbc:calcite:model=target/classes/weblogs-model.json admin admin
```



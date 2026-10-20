# Pipeline B — Real-time event processing

## Run locally

1. Start Kafka:
   ```
   docker compose -f streaming/docker-compose.yml up -d
   ```

2. Generate sample data if you haven't (`data/sample_order_events.csv`):
   ```
   python data/generate_sample_data.py
   ```

3. Start the producer:
   ```
   python streaming/event_producer.py --rate 5 --duration 120
   ```

4. Start the Spark consumer (writes deduped events to Delta Lake by default).
   Run it with plain `python`, not `spark-submit` — `build_spark()` resolves
   the Kafka connector and Delta coordinates at runtime to match whatever
   `pyspark`/`delta-spark` version is actually installed (`requirements.txt`
   pins neither), instead of a hardcoded version pair that breaks the moment
   a newer release is installed. That resolution needs to happen before the
   JVM starts, which only works when `python` itself bootstraps the JVM —
   `spark-submit script.py` starts its JVM first, too early for packages
   configured from inside the script to take effect:
   ```
   python streaming/spark_consumer.py --sink delta
   ```

5. Verify output and dedup. A bare `spark` session (e.g. a plain `pyspark`
   shell) doesn't have Delta's data source registered, so
   `spark.read.format("delta").load(...)` on its own fails with
   `[DATA_SOURCE_NOT_FOUND]`. Reuse `build_spark()` instead, which wires up
   the same Delta config the consumer ran with:
   ```
   python -c "
   from streaming.spark_consumer import build_spark
   spark = build_spark()
   df = spark.read.format('delta').load('streaming/output/order_events')
   print('total:', df.count())
   print('distinct event_ids:', df.select('event_id').distinct().count())
   "
   ```
   The two counts should be equal (every row has a distinct `event_id`,
   duplicates dropped), and `total` should be less than the events sent by
   the producer.

## Recovery test

Kill the `python streaming/spark_consumer.py` process mid-run and restart it
with the same
`--checkpoint-path`. It should resume from the last committed Kafka offset
without reprocessing already-written events or losing in-flight ones —
this is what `tests/test_streaming_dedup.py` exercises at the function level,
and what to demo live for the "reliability" requirement in PLAN.md.

## Switching to Kinesis (optional/stretch)

Replace the `readStream.format("kafka")` call in `spark_consumer.py` with
the `kinesis` connector equivalent, point `event_producer.py` at a Kinesis
`put_record` call via boto3 instead of `KafkaProducer`, and keep the Spark
dedup/watermark logic unchanged — it's source-agnostic.

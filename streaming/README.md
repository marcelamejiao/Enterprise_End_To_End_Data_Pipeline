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

4. Start the Spark consumer (writes deduped events to Delta Lake by default):
   ```
   spark-submit \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
       streaming/spark_consumer.py --sink delta
   ```

5. Verify output and dedup:
   ```python
   spark.read.format("delta").load("streaming/output/order_events").count()
   ```
   Count should be less than events sent by the producer (duplicates dropped),
   and every row should have a distinct `event_id`.

## Recovery test

Kill the `spark-submit` process mid-run and restart it with the same
`--checkpoint-path`. It should resume from the last committed Kafka offset
without reprocessing already-written events or losing in-flight ones —
this is what `tests/test_streaming_dedup.py` exercises at the function level,
and what to demo live for the "reliability" requirement in PLAN.md.

## Switching to Kinesis (optional/stretch)

Replace the `readStream.format("kafka")` call in `spark_consumer.py` with
the `kinesis` connector equivalent, point `event_producer.py` at a Kinesis
`put_record` call via boto3 instead of `KafkaProducer`, and keep the Spark
dedup/watermark logic unchanged — it's source-agnostic.

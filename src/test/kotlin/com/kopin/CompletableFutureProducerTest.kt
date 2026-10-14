package com.kopin

import com.kopin.TestKafka.consume
import com.kopin.TestKafka.stringProducer
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.CompletableFuture
import kotlin.test.Test

class CompletableFutureProducerTest {

    companion object {
        private const val TOPIC = "future-single-record"
        private const val BATCH_TOPIC = "future-batch"
        private const val BATCH_SIZE = 100
    }

    fun <K, V> Producer<K, V>.sendAsync(record: ProducerRecord<K, V>): CompletableFuture<RecordMetadata> =
        CompletableFuture<RecordMetadata>().also { future ->
            send(record) { metadata, exception ->
                if (exception != null) future.completeExceptionally(exception)
                else future.complete(metadata)
            }
        }

    @Test
    fun `single record is delivered with its key and value`() {
        stringProducer().use { producer ->
            val metadata = producer.sendAsync(ProducerRecord(TOPIC, "key-1", "hello kafka")).join()
            metadata.topic() shouldBe TOPIC
        }

        val records = consume(TOPIC, expected = 1)
        records shouldHaveSize 1
        records.single().key() shouldBe "key-1"
        records.single().value() shouldBe "hello kafka"
    }

    @Test
    fun `batch of records sent concurrently is fully delivered`() {
        val sent = (1..BATCH_SIZE).map { i -> "key-$i" to "message-$i" }

        stringProducer().use { producer ->
            val futures = sent.map { (key, value) -> producer.sendAsync(ProducerRecord(BATCH_TOPIC, key, value)) }
            CompletableFuture.allOf(*futures.toTypedArray()).join()
            futures.map { it.join().topic() }.toSet() shouldBe setOf(BATCH_TOPIC)
        }

        val records = consume(BATCH_TOPIC, expected = BATCH_SIZE)

        records shouldHaveSize BATCH_SIZE
        records.map { it.key() to it.value() } shouldContainExactlyInAnyOrder sent
    }
}

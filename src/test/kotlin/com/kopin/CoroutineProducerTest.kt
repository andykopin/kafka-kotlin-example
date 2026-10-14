package com.kopin

import com.kopin.TestKafka.consume
import com.kopin.TestKafka.stringProducer
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.test.Test

class CoroutineProducerTest {

    companion object {
        private const val TOPIC = "coroutine-single-record"
        private const val BATCH_TOPIC = "coroutine-batch"
        private const val BATCH_SIZE = 100
    }

    suspend fun <K, V> Producer<K, V>.sendAwait(record: ProducerRecord<K, V>): RecordMetadata =
        suspendCancellableCoroutine { cont ->
            send(record) { metadata, exception ->
                if (exception != null) cont.resumeWithException(exception)
                else cont.resume(metadata)
            }
        }

    @Test
    fun `single record is delivered with its key and value`() = runTest {
        stringProducer().use { producer ->
            val metadata = producer.sendAwait(ProducerRecord(TOPIC, "key-1", "hello kafka"))
            metadata.topic() shouldBe TOPIC
        }

        val records = consume(TOPIC, expected = 1)
        records shouldHaveSize 1
        records.single().key() shouldBe "key-1"
        records.single().value() shouldBe "hello kafka"
    }

    @Test
    fun `batch of records sent concurrently is fully delivered`() = runTest {
        val sent = (1..BATCH_SIZE).map { i -> "key-$i" to "message-$i" }

        stringProducer().use { producer ->
            val metadata = sent
                .map { (key, value) -> async { producer.sendAwait(ProducerRecord(BATCH_TOPIC, key, value)) } }
                .awaitAll()
            metadata.map { it.topic() }.toSet() shouldBe setOf(BATCH_TOPIC)
        }

        val records = consume(BATCH_TOPIC, expected = BATCH_SIZE)

        records shouldHaveSize BATCH_SIZE
        records.map { it.key() to it.value() } shouldContainExactlyInAnyOrder sent
    }
}

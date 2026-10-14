package com.kopin

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration

/**
 * Single Kafka container shared by all test classes: started on first access,
 * stopped by Testcontainers (Ryuk) when the JVM exits.
 */
object TestKafka {

    private val container = KafkaContainer("apache/kafka:4.3.1").apply { start() }

    private val commonConfig: Map<String, Any>
        get() = mapOf(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to container.bootstrapServers,
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to SecurityProtocol.PLAINTEXT.name,
        )

    fun stringProducer() = KafkaProducer<String, String>(
        commonConfig + mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
        )
    )

    fun stringConsumer(groupId: String) = KafkaConsumer<String, String>(
        commonConfig + mapOf(
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        )
    )

    /** Polls [topic] from the beginning until [expected] records arrive or [timeout] elapses. */
    fun consume(
        topic: String,
        expected: Int,
        timeout: Duration = Duration.ofSeconds(10),
    ): List<ConsumerRecord<String, String>> =
        stringConsumer("$topic-group").use { consumer ->
            consumer.subscribe(listOf(topic))
            buildList {
                val deadline = System.nanoTime() + timeout.toNanos()
                while (size < expected && System.nanoTime() < deadline) {
                    addAll(consumer.poll(Duration.ofMillis(500)))
                }
            }
        }
}

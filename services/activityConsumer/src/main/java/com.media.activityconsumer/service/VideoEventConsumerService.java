package com.media.activityconsumer.service;

import com.media.activityconsumer.model.VideoEvent;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class VideoEventConsumerService {

    private static final Logger log = LoggerFactory.getLogger(VideoEventConsumerService.class);

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private volatile boolean hasProcessedBatch = false;

    @Value("${kafka.batch.interval.ms:5000}")
    private long batchIntervalMs;

    @Value("${kafka.batch.max-wait.ms:4000}")
    private long maxWaitMs;

    @KafkaListener(
            id = "videoEventListener",
            topics = "${spring.kafka.consumer.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory",
            autoStartup = "false"
    )
    public void consumeVideoEvents(
            @Payload List<VideoEvent> events,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets
    ) {
        try {
            long startTime = System.currentTimeMillis();

            log.info("========================================");
            log.info("Processing batch from partition: {}, offsets: {}-{}",
                    partition, offsets.get(0), offsets.get(offsets.size() - 1));
            log.info("Received batch of size: {}", events.size());
            log.info("========================================");

            processEvents(events);

            long processingTime = System.currentTimeMillis() - startTime;
            log.info("Batch processed successfully in {}ms", processingTime);
            hasProcessedBatch = true;

        } catch (Exception e) {
            log.error("Error processing batch: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void processEvents(List<VideoEvent> events) {
        for (VideoEvent event : events) {
            log.info("Processing VideoEvent: {}", event);
            // TODO: Add your business logic here
        }
    }

    @Scheduled(fixedDelayString = "${kafka.batch.interval.ms:5000}",
            initialDelayString = "${kafka.batch.initial-delay.ms:5000}")
    public void scheduledBatchConsumption() {
        if (!isProcessing.compareAndSet(false, true)) {
            log.warn("Previous batch still processing, skipping this cycle");
            return;
        }

        MessageListenerContainer container = null;

        try {
            log.info("");
            log.info("================================================");
            log.info("⏰ Scheduled batch consumption triggered");
            log.info("================================================");

            container = registry.getListenerContainer("videoEventListener");

            if (container == null) {
                log.error("Kafka listener container not found!");
                return;
            }

            if (!container.isRunning()) {
                log.info("▶️  Starting Kafka consumer...");
                container.start();
            }

            hasProcessedBatch = false;

            long startWait = System.currentTimeMillis();
            long waitedMs = 0;

            while (!hasProcessedBatch && waitedMs < maxWaitMs) {
                Thread.sleep(100);
                waitedMs = System.currentTimeMillis() - startWait;
            }

            if (hasProcessedBatch) {
                log.info("✅ Batch processing detected, waiting for completion...");
                Thread.sleep(500);
            } else {
                log.info("ℹ️  No messages available in this cycle");
            }

        } catch (InterruptedException e) {
            log.error("Batch processing interrupted", e);
            Thread.currentThread().interrupt();

        } catch (Exception e) {
            log.error("Error in scheduled batch processing", e);

        } finally {
            stopContainer(container);
            isProcessing.set(false);

            log.info("================================================");
            log.info("✅ Scheduled batch consumption completed");
            log.info("⏰ Next run in {}ms", batchIntervalMs);
            log.info("================================================");
            log.info("");
        }
    }

    private void stopContainer(MessageListenerContainer container) {
        try {
            if (container != null && container.isRunning()) {
                log.info("⏸️  Pausing Kafka consumer until next cycle");
                container.stop();
            }
        } catch (Exception e) {
            log.error("Error stopping container", e);
        }
    }

    @PostConstruct
    public void init() {
        log.info("");
        log.info("================================================");
        log.info("🚀 Batch Consumer Service Initialized");
        log.info("📊 Polling Interval: {}ms ({}s)", batchIntervalMs, batchIntervalMs / 1000.0);
        log.info("📦 Max Batch Size: 100 messages");
        log.info("⏱️  Max Processing Wait: {}ms", maxWaitMs);
        log.info("⏰ First poll in {}ms...", batchIntervalMs);
        log.info("================================================");
        log.info("");
    }
}
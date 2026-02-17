package com.tcarroll10.throughPut_client.service;

import com.tcarroll10.throughPut_client.to.AccountingResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class RecordSenderService {

    private final long totalRecords;
    private final int batchSize;
    private final int threadCount;
    private final String targetUrl;

    private final RestTemplate restTemplate;
    private final ExecutorService executor;

    public RecordSenderService(
            RestTemplate restTemplate,
            @Value("${throughput.total-records}") long totalRecords,
            @Value("${throughput.batch-size}") int batchSize,
            @Value("${throughput.thread-count}") int threadCount,
            @Value("${throughput.target-url}") String targetUrl) {

        this.restTemplate = restTemplate;
        this.totalRecords = totalRecords;
        this.batchSize = batchSize;
        this.threadCount = threadCount;
        this.targetUrl = targetUrl;
        this.executor = Executors.newFixedThreadPool(threadCount);
    }

    public void sendAll() {
        long totalBatches = totalRecords / batchSize;
        AtomicInteger sent = new AtomicInteger(0);
        AtomicInteger failedBatches = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch((int) totalBatches);

        log.info("========================================");
        log.info("       TEST CONFIGURATION");
        log.info("========================================");
        log.info("Total records:  {}", String.format("%,d", totalRecords));
        log.info("Batch size:     {}", String.format("%,d", batchSize));
        log.info("Total batches:  {}", String.format("%,d", totalBatches));
        log.info("Thread count:   {}", threadCount);
        log.info("Target URL:     {}", targetUrl);
        log.info("Endpoint type:  {}", batchSize == 1 ? "SINGLE record" : "BATCH");
        log.info("Auth enabled:   YES");
        log.info("========================================");

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalBatches; i++) {
            final int batchNum = i;
            executor.submit(() -> {
                try {
                    // Create headers with API key
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    headers.set("X-API-Key", "test-api-key-12345");

                    if (batchSize == 1) {
                        // Send single record directly
                        AccountingResult record = new AccountingResult(
                                String.valueOf(batchNum),
                                "payload-" + batchNum,
                                System.currentTimeMillis());

                        HttpEntity<AccountingResult> request = new HttpEntity<>(record, headers);

                        restTemplate.postForEntity(targetUrl, request, Void.class);
                        sent.incrementAndGet();
                    } else {
                        // Send batch
                        List<AccountingResult> batch = generateBatch(batchNum * batchSize, batchSize);

                        HttpEntity<List<AccountingResult>> request = new HttpEntity<>(batch, headers);

                        restTemplate.postForEntity(targetUrl, request, Void.class);
                        int totalSent = sent.addAndGet(batchSize);

                        int logInterval = totalRecords >= 50_000_000 ? 1_000_000 : 500_000;
                        if (totalSent % logInterval == 0) {
                            long elapsed = System.currentTimeMillis() - startTime;
                            double rate = (totalSent / (elapsed / 1000.0));
                            log.info("Progress: {} / {} records ({}%) - Rate: {} records/sec",
                                    String.format("%,d", totalSent),
                                    String.format("%,d", totalRecords),
                                    String.format("%.1f", totalSent * 100.0 / totalRecords),
                                    String.format("%.0f", rate));
                        }
                    }
                } catch (Exception e) {
                    failedBatches.incrementAndGet();
                    if (failedBatches.get() <= 10) {
                        log.error("Batch {} failed: {}", batchNum, e.getMessage());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            double minutes = totalTime / 60000.0;
            double seconds = totalTime / 1000.0;
            double avgRate = totalRecords / seconds;

            log.info("========================================");
            log.info("            RESULTS");
            log.info("========================================");
            log.info("Batch size:        {} records/request", String.format("%,d", batchSize));
            log.info("Total records:     {}", String.format("%,d", totalRecords));
            log.info("Total requests:    {}", String.format("%,d", totalBatches));
            log.info("Failed requests:   {}", failedBatches.get());
            log.info("Success rate:      {}%",
                    String.format("%.2f", (totalBatches - failedBatches.get()) * 100.0 / totalBatches));
            log.info("Total time:        {} minutes ({} seconds)", String.format("%.2f", minutes),
                    String.format("%.1f", seconds));
            log.info("Average rate:      {} records/second", String.format("%,.0f", avgRate));
            log.info("Requests/second:   {}", String.format("%.0f", totalBatches / seconds));
            log.info("========================================");
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for completion", e);
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdown();
        }
    }

    private List<AccountingResult> generateBatch(int startId, int size) {
        List<AccountingResult> batch = new ArrayList<>(size);
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            batch.add(new AccountingResult(
                    String.valueOf(startId + i),
                    "payload-" + (startId + i),
                    timestamp));
        }
        return batch;
    }
}
package com.tcarroll10.throughPut_client.service;

import com.tcarroll10.throughPut_client.to.AccountingResult;
import lombok.extern.slf4j.Slf4j;
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

    private static final long TOTAL_RECORDS = 70_000_000L;
    private static final int BATCH_SIZE = 1_000;
    private static final int THREAD_COUNT = 50;
    private static final String TARGET_URL = 
        "http://abf2e905b3ac04e2a89ccdd50f5a5906-1023625363.us-east-1.elb.amazonaws.com/api/records/batch";

    private final RestTemplate restTemplate;
    private final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

    public RecordSenderService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public void sendAll() {
        long totalBatches = TOTAL_RECORDS / BATCH_SIZE;
        AtomicInteger sent = new AtomicInteger(0);
        AtomicInteger failedBatches = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch((int) totalBatches);

        log.info("Starting to send {} records in {} batches using {} threads", 
            TOTAL_RECORDS, totalBatches, THREAD_COUNT);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalBatches; i++) {
            final int batchNum = i;
            executor.submit(() -> {
                try {
                    List<AccountingResult> batch = generateBatch(batchNum * BATCH_SIZE, BATCH_SIZE);
                    restTemplate.postForEntity(TARGET_URL, batch, Void.class);
                    
                    int totalSent = sent.addAndGet(BATCH_SIZE);
                    if (totalSent % 1_000_000 == 0) {
                        long elapsed = System.currentTimeMillis() - startTime;
                        double rate = (totalSent / (elapsed / 1000.0));
                        log.info("Progress: {:,} / {:,} records ({:.1f}%) - Rate: {:.0f} records/sec", 
                            totalSent, TOTAL_RECORDS, 
                            (totalSent * 100.0 / TOTAL_RECORDS),
                            rate);
                    }
                } catch (Exception e) {
                    failedBatches.incrementAndGet();
                    log.error("Batch {} failed: {}", batchNum, e.getMessage());
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
            double avgRate = TOTAL_RECORDS / (totalTime / 1000.0);

            log.info("===== COMPLETE =====");
            log.info("Total records sent: {:,}", TOTAL_RECORDS);
            log.info("Failed batches: {}", failedBatches.get());
            log.info("Total time: {:.2f} minutes ({} seconds)", minutes, totalTime / 1000);
            log.info("Average rate: {:.0f} records/second", avgRate);
            log.info("====================");
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
                timestamp
            ));
        }
        return batch;
    }
} 

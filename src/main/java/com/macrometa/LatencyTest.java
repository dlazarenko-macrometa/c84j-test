/**
 * Copyright (c) 2024 Macrometa Corp All rights reserved.
 */
package com.macrometa;

import com.c8db.C8Collection;
import com.c8db.C8DB;
import com.c8db.C8Database;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LatencyTest {

    private static final int PARALLELISM = 1;
    private static final int COUNT_ITERATIONS = 100;
    private static final String HOST = "api-dmytro-us-west.eng.macrometa.io";
    private static final String TENANT = "demo";
    private static final String FABRIC = "_system";
    private static final String COLLECTION_NAME = "lat_test";
    private static final String COLLECTION_DOC_KEY = "1";
    private static final String API_KEY =
            "demo.lat_test.5rWIWYGwwMeDdlrAZF2EuvPylR8QEYE7ORD93imaZvoRJFGSojbI0aDe5eWbi3SJ12942f";

    private static final Logger log = LogManager.getLogger(LatencyTest.class);
    private static ExecutorService executor;


    public static void main(String[] args) {
        log.info("Init Tests...");
        log.info("PARALLELISM = {}", PARALLELISM);
        log.info("COUNT_ITERATIONS = {}", COUNT_ITERATIONS);
        log.info("HOST = {}", HOST);
        log.info("TENANT = {}", TENANT);
        log.info("FABRIC = {}", FABRIC);
        log.info("COLLECTION_NAME = {}", COLLECTION_NAME);
        log.info("COLLECTION_DOC_KEY = {}", COLLECTION_DOC_KEY);
        log.info("API_KEY = {}", API_KEY);
        executor = Executors.newWorkStealingPool(PARALLELISM);

        C8DB.Builder clusterBuilder = new C8DB.Builder()
                .host(HOST, 443)
                .useSsl(true)
                .apiKey(API_KEY);
        C8DB c8db = clusterBuilder.build();
        C8Database c8Database = c8db.db(TENANT, FABRIC);
        C8Collection collection = c8Database.collection(COLLECTION_NAME);

        log.info("Tests started..");
        CountDownLatch latch = new CountDownLatch(COUNT_ITERATIONS);
        AtomicLong sum = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);

        for (int i = 0; i < COUNT_ITERATIONS; i++) {
            final int ifinal = i;
            executor.execute(() -> {
                StopWatch watch = new StopWatch();
                watch.start();
                try {
                    Map<String, Object> doc = collection.getDocument(COLLECTION_DOC_KEY, Map.class);
                } catch (Exception e) {
                    log.error("Error in a loop {}", ifinal, e);
                    errors.incrementAndGet();
                }
                watch.stop();
                sum.addAndGet(watch.getTime(TimeUnit.MILLISECONDS));
                latch.countDown();
            });

        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        log.info("Tests completed. Count errors: {}. Avg Latency: {} milliseconds",
                errors.get(), (sum.get() / COUNT_ITERATIONS));
    }

}

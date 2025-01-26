package org.oba.jmongolock;

import com.mongodb.client.MongoClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.oba.jmongolock.IntegrationTestUtils.createMongoClient;

public class JMongoLockAdvancedIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JMongoLockAdvancedIntegrationTest.class);


    private final AtomicBoolean intoCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean errorInCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean otherError = new AtomicBoolean(false);
    private String lockName;
    private final List<JMongoLock> lockList = new ArrayList<>();
    private final List<MongoClient> mongoClients = new ArrayList<>();

    @Test
    public void testIfInterruptedFor5SecondsLock() throws InterruptedException {
        for(int i = 0; i < 1; i++) {
            lockName = "lock_" + this.getClass().getName() + "_" + Long.toHexString(System.currentTimeMillis());
            intoCriticalZone.set(false);
            errorInCriticalZone.set(false);
            otherError.set(false);
            LOGGER.info("_\n");
            LOGGER.info("i {}", i);
            Thread t1 = new Thread(() -> accessLockOfCriticalZone(1));
            t1.setName("prueba_t1");
            Thread t2 = new Thread(() -> accessLockOfCriticalZone(7));
            t2.setName("prueba_t2");
            Thread t3 = new Thread(() -> accessLockOfCriticalZone(3));
            t3.setName("prueba_t3");
            List<Thread> threadList = Arrays.asList(t1,t2,t3);
            Collections.shuffle(threadList);
            threadList.forEach(Thread::start);
            t1.join();
            t2.join();
            t3.join();
            assertFalse(errorInCriticalZone.get());
            assertFalse(otherError.get());
            assertTrue(lockList.stream().anyMatch(il -> il != null && !il.checkLock()));
            mongoClients.forEach( mongoClient -> mongoClient.close());
        }
    }

    private void accessLockOfCriticalZone(int sleepTime) {
        try {
            MongoClient mongoClient = createMongoClient();
            mongoClients.add(mongoClient);
            JMongoLock jMongoLock = new JMongoLock(mongoClient, lockName);
            lockList.add(jMongoLock);
            if (jMongoLock.tryLock()) {
                LOGGER.debug("accessLockOfCriticalZone inside for jmongolock {}", jMongoLock );
                accessCriticalZone(sleepTime);
                jMongoLock.unLock();
            } else {
                LOGGER.debug("accessLockOfCriticalZone not accesed for jmongolock {}", jMongoLock );
            }
        } catch (Exception e) {
            LOGGER.error("Other error ", e);
            otherError.set(true);
        }
    }

    private void accessCriticalZone(int sleepTime){
        if (intoCriticalZone.get()) {
            errorInCriticalZone.set(true);
            throw new IllegalStateException("Other thread is here");
        }
        intoCriticalZone.set(true);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTime));
        } catch (InterruptedException e) {
            //NOPE
        }
        intoCriticalZone.set(false);
    }

}

package org.oba.jmongolock;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.oba.jmongolock.IntegrationTestUtils.withMongoClient;
import static org.oba.jmongolock.IntegrationTestUtils.withMongoClientGet;

public class JMongoLockSimpleIntegrationTest {



    private Long getData(String name, String field) {
        return withMongoClientGet(mongoClient1 -> {
            Bson filter = Filters.eq("name", name);
            Document result = mongoClient1.getDatabase("locks").getCollection("lock").find(filter).first();
            return result == null ? 0L : result.getLong(field);
        });
    }

    @Test
    public void basic1Test() {
        String ts = Long.toHexString(System.currentTimeMillis());
        String testLockName = "JMongoLockSimpleIntegrationTest1_" + ts;
        withMongoClient(mongoClient -> {
            JMongoLock mongoLock1 = new JMongoLock(mongoClient, testLockName);
            JMongoLock mongoLock2 = new JMongoLock(mongoClient, testLockName);
            boolean result_1_1 = mongoLock1.tryLock();
            boolean checkLock_1_1 = mongoLock1.checkLock();
            long ts1 = getData(testLockName, "lastAccessed");
            boolean result_2_1 = mongoLock2.tryLock();
            long ts2 = getData(testLockName, "lastAccessed");
            boolean unlock_1_1 = mongoLock1.unLock();
            boolean checkLock_1_2 = mongoLock1.checkLock();
            assertTrue(result_1_1);
            assertTrue(checkLock_1_1);
            assertFalse(result_2_1);
            assertTrue(unlock_1_1);
            assertFalse(checkLock_1_2);
            assertTrue(ts2 > ts1 && ts2 != 0 && ts1 != 0);

        });
    }


    @Test
    public void basic2Test() {
        String ts = Long.toHexString(System.currentTimeMillis());
        String testLockName = "JMongoLockSimpleIntegrationTest2_" + ts;
        withMongoClient(mongoClient -> {
            JMongoLock mongoLock1 = new JMongoLock(mongoClient, testLockName);
            JMongoLock mongoLock2 = new JMongoLock(mongoClient, testLockName);
            boolean result_1_1 = mongoLock1.tryLock();
            boolean checkLock_1_1 = mongoLock1.checkLock();
            boolean result_2_1 = mongoLock2.tryLock();
            boolean unlock_1_1 = mongoLock1.unLock();
            boolean result_2_2 = mongoLock2.tryLock();
            boolean checkLock_2_1 = mongoLock2.checkLock();
            boolean checkLock_1_2 = mongoLock1.checkLock();
            boolean unlock_1_2 = mongoLock1.unLock();
            boolean unlock_2_1 = mongoLock2.unLock();
            boolean unlock_2_2 = mongoLock2.unLock();
            boolean checkLock_1_3 = mongoLock1.checkLock();
            assertTrue(result_1_1);
            assertTrue(checkLock_1_1);
            assertFalse(result_2_1);
            assertTrue(unlock_1_1);
            assertTrue(result_2_2);
            assertTrue(checkLock_2_1);
            assertFalse(unlock_1_2);
            assertTrue(unlock_2_1);
            assertFalse(unlock_2_2);
            assertFalse(checkLock_1_2);
            assertFalse(checkLock_1_3);
        });
    }

    @Test
    public void basic3Test() {

        long millisOfLock = 30_000L;
        String ts = Long.toHexString(System.currentTimeMillis());
        String testLockName = "JMongoLockSimpleIntegrationTest3_" + ts;
        withMongoClient(mongoClient -> {
            JMongoLock mongoLock1 = new JMongoLock(mongoClient, testLockName, millisOfLock, TimeUnit.MILLISECONDS);
            boolean result_1_1 = mongoLock1.tryLock();
            try {
                Thread.sleep(millisOfLock + 120L);
            } catch (InterruptedException e) {
                // empty
            }
//            boolean checkLock_1_1 = mongoLock1.checkLock();
            assertTrue(result_1_1);
//            assertFalse(checkLock_1_1);
        });
    }

}

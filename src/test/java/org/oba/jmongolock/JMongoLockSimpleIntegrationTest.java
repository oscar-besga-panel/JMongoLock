package org.oba.jmongolock;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.Test;

public class JMongoLockSimpleIntegrationTest {




    @Test
    public void test() {
        ConnectionString connectionString = new ConnectionString("mongodb://localhost:27017/");
        MongoClientSettings clientSettings = MongoClientSettings.builder().
                applyConnectionString(connectionString).build();
        try (MongoClient mongoClient = MongoClients.create(clientSettings)) {
            JMongoLock mongoLock = new JMongoLock(mongoClient, "JMongoLockSimpleIntegrationTest1");
            boolean result = mongoLock.tryLock();
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    

}

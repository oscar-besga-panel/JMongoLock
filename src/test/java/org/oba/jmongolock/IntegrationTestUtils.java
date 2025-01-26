package org.oba.jmongolock;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.util.function.Consumer;
import java.util.function.Function;

public class IntegrationTestUtils {



    static MongoClient createMongoClient() {
        ConnectionString connectionString = new ConnectionString("mongodb://localhost:27017/");
        MongoClientSettings clientSettings = MongoClientSettings.builder().
                applyConnectionString(connectionString).build();
        return MongoClients.create(clientSettings);
    }

    static void withMongoClient(Consumer<MongoClient> action) {
        try (MongoClient mongoClient = createMongoClient()) {
            action.accept(mongoClient);
        }
    }

    static <T> T withMongoClientGet(Function<MongoClient, T> action) {
        try (MongoClient mongoClient = createMongoClient()) {
            return action.apply(mongoClient);
        }
    }


}

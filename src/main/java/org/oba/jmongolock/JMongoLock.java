package org.oba.jmongolock;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class JMongoLock implements Closeable, AutoCloseable {

    private final static String DEFAULT_DATABASE = "locks";
    private final static String DEFAULT_COLLECTION = "lock";
    private final static Long DEFAULT_TIMEOUT = 36500L;
    private final static TimeUnit DEFAULT_TIMEOUT_UNIT = TimeUnit.DAYS;

    private final static FindOneAndUpdateOptions FIND_ONE_AND_UPDATE_OPTIONS = new FindOneAndUpdateOptions().
            returnDocument(ReturnDocument.AFTER).
            upsert(true);

    public static final String NAME = "name";
    public static final String TOKEN = "token";
    public static final String EXPIRES = "expires";
    public static final String ACCESSED = "accessed";

    private synchronized static String generateToken() {
        return String.join("_", String.valueOf(System.currentTimeMillis()), String.valueOf(ThreadLocalRandom.current().nextLong()));
    }

    private final MongoClient mongoClient;
    private final String database;
    private final String collection;
    private final String name;
    private final String token;
    private final long expirationTime;
    private final TimeUnit expirationTimeUnit;


    public JMongoLock(MongoClient mongoClient, String name) {
        this(mongoClient, DEFAULT_DATABASE, DEFAULT_COLLECTION, name, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT_UNIT);
    }

    public JMongoLock(MongoClient mongoClient, String name, long expirationTime, TimeUnit expirationTimeUnit) {
        this(mongoClient, DEFAULT_DATABASE, DEFAULT_COLLECTION, name, expirationTime, expirationTimeUnit);
    }

    public JMongoLock(MongoClient mongoClient, String database, String collection, String name) {
        this(mongoClient, database, collection, name, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT_UNIT);
    }

    public JMongoLock(MongoClient mongoClient, String database, String collection, String name, long expirationTime, TimeUnit expirationTimeUnit) {
        this.mongoClient = mongoClient;
        this.database = database;
        this.collection = collection;
        this.name = name;
        this.token = generateToken();
        this.expirationTime = expirationTime;
        this.expirationTimeUnit = expirationTimeUnit;
    }

    public String getName() {
        return name;
    }

    public String getToken() {
        return token;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public TimeUnit getExpirationTimeUnit() {
        return expirationTimeUnit;
    }

    MongoCollection<Document> getCurrentMongoCollection() {
        return mongoClient.getDatabase(database).getCollection(collection);
    }


    public boolean tryLock() {
        // https://stackoverflow.com/questions/60035042/mongodb-document-update-array-element-using-findoneandupdate-method-in-java
        // https://www.baeldung.com/mongodb-update-documents
        // https://www.geeksforgeeks.org/mongodb-db-collection-findoneandupdate-method/
        // https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/crud/compound-operations/#find-and-update

        Bson nameFilter = Filters.eq(NAME, name);

        Long expire = Long.valueOf(System.currentTimeMillis() + expirationTimeUnit.toMillis(expirationTime));
        Long accessed = Long.valueOf(System.currentTimeMillis());
        Bson insertName = Updates.setOnInsert(NAME, name);
        Bson insertToken = Updates.setOnInsert(TOKEN, token);
        Bson insertExpires = Updates.setOnInsert(EXPIRES, expire);
        Bson updateAccessed = Updates.set(ACCESSED, accessed );
        Bson updates = Updates.combine(insertName, insertToken, insertExpires, updateAccessed);

//        FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().
//                arrayFilters(arrayFilters).
//                upsert(true);

        MongoCollection<Document> mongoCollection = getCurrentMongoCollection();
        Document result = mongoCollection.findOneAndUpdate(nameFilter, updates, FIND_ONE_AND_UPDATE_OPTIONS);
        return result != null && token.equals(result.get(TOKEN));
    }


    public boolean checkLock() {
        Bson nameFilter = Filters.eq(NAME, name);
        MongoCollection<Document> mongoCollection = getCurrentMongoCollection();
        Document result = mongoCollection.find(nameFilter).first();
        return result != null && token.equals(result.get(TOKEN));
    }

    public boolean unLock() {
        Bson nameFilter = Filters.eq(NAME, name);
        Bson tokenFilter = Filters.eq(TOKEN, token);
        MongoCollection<Document> mongoCollection = getCurrentMongoCollection();
        Document result = mongoCollection.findOneAndDelete(Filters.and(nameFilter, tokenFilter));
        return result != null;
    }

    @Override
    public void close() {
        unLock();
    }

    public void withLockDo(Runnable action) {
        try(this) {
            action.run();
        }
    }

    public <T> T withLockGet(Supplier<T> action) {
        try(this) {
            return action.get();
        }
    }


}

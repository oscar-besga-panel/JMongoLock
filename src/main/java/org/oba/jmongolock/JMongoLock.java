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
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class JMongoLock implements Closeable, AutoCloseable {

    private final static String DEFAULT_DATABASE = "lock";
    private final static String DEFAULT_COLLECTION = "locks";
    private final static Long DEFAULT_TIMEOUT = 36500L; // 1 year lock by default
    private final static TimeUnit DEFAULT_TIMEOUT_UNIT = TimeUnit.DAYS;

    private final static FindOneAndUpdateOptions FIND_ONE_AND_UPDATE_OPTIONS = new FindOneAndUpdateOptions().
            returnDocument(ReturnDocument.AFTER).
            upsert(true);

    public static final String NAME = "name";
    public static final String TOKEN = "token";
    public static final String EXPIRE_AT = "expireAt";
    public static final String CREATED = "created";
    public static final String LAST_ACCESSED = "lastAccessed";

    private synchronized static String generateToken() {
        return String.join("_", String.valueOf(System.currentTimeMillis()), Long.toHexString(ThreadLocalRandom.current().nextLong()));
    }

    private final MongoClient mongoClient;
    private final String database;
    private final String collection;
    private final String name;
    private final String token;
    private final long expirationTime;
    private final TimeUnit expirationTimeUnit;
    private long created;
    private long lastAccessed;


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

    public long getLastAccessed() {
        return lastAccessed;
    }

    public long getCreated() {
        return created;
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
        long ts = System.currentTimeMillis();
        long expire =  ts + expirationTimeUnit.toMillis(expirationTime);
        lastAccessed = ts;
        if (created == 0) {
            created = ts;
        }
        Bson insertName = Updates.setOnInsert(NAME, name);
        Bson insertToken = Updates.setOnInsert(TOKEN, token);
        Bson insertExpireAt = Updates.setOnInsert(EXPIRE_AT, new Date(expire));
        Bson insertCreated = Updates.setOnInsert(CREATED, new Date(created));
        Bson updateAccessed = Updates.set(LAST_ACCESSED, new Date(lastAccessed) );
        Bson updates = Updates.combine(insertName, insertToken, insertExpireAt, insertCreated, updateAccessed);

//        FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().
//                arrayFilters(arrayFilters).
//                upsert(true);

        MongoCollection<Document> mongoCollection = getCurrentMongoCollection();
        Document result = mongoCollection.findOneAndUpdate(nameFilter, updates, FIND_ONE_AND_UPDATE_OPTIONS);
        return checkResultDocument(result);
    }


    public boolean checkLock() {
        Bson nameFilter = Filters.eq(NAME, name);
        Bson tokenFilter = Filters.eq(TOKEN, token);
        Bson checkFilter = Filters.and(nameFilter, tokenFilter);
        MongoCollection<Document> mongoCollection = getCurrentMongoCollection();
        Document result = mongoCollection.find(checkFilter).first();
        return checkResultDocument(result);
    }

    public boolean unLock() {
        Bson nameFilter = Filters.eq(NAME, name);
        Bson tokenFilter = Filters.eq(TOKEN, token);
        Bson deleteFilter = Filters.and(nameFilter, tokenFilter);
        MongoCollection<Document> mongoCollection = getCurrentMongoCollection();
        Document result = mongoCollection.findOneAndDelete(deleteFilter);
        return checkResultDocument(result);
    }

    private boolean checkResultDocument(Document result) {
        return result != null && token.equals(result.get(TOKEN));
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


    public boolean sameLock(JMongoLock other) {
        if (other == null ) {
            return false;
        } else {
            return Objects.equals(database, other.database) && Objects.equals(collection, other.collection) &&
                    Objects.equals(name, other.name);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        JMongoLock mongoLock = (JMongoLock) o;
        return Objects.equals(database, mongoLock.database) && Objects.equals(collection, mongoLock.collection) &&
                Objects.equals(name, mongoLock.name) && Objects.equals(token, mongoLock.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, collection, name, token);
    }

    @Override
    public String toString() {
        return "JMongoLock{" +
                "database='" + database + '\'' +
                ", collection='" + collection + '\'' +
                ", name='" + name + '\'' +
                ", token='" + token + '\'' +
                ", expirationTime=" + expirationTime +
                ", expirationTimeUnit=" + expirationTimeUnit +
                ", lastAccessed=" + lastAccessed +
                '}';
    }

}

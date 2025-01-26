
// mongosh "mongodb://localhost:27017" JMongoLock/src/main/mongo/create_lock_collection.js

var log = "";

print("Using database lock");
db = db.getSiblingDB("lock")

if (db.getCollection("locks") != null) {
    print("Removing old collection locks");
    log = db.getCollection("locks").drop();
    print(log);
}
print("Creating collection locks");
log = db.createCollection("locks", {
    validator: {
        $jsonSchema: {
            description: "Lock collection",
            bsonType: "object",
            required: [ "name", "token", "expireAt", "lastAccessed", "created" ],
            properties: {
                name: {
                   bsonType: "string",
                   description: "name of the lock, indexed and unique"
                },
                token: {
                   bsonType: "string",
                   description: "token of the actual holder"
                },
                expireAt: {
                   bsonType: "date",
                   description: "when will this token die"
                },
                lastAccessed: {
                   bsonType: "date",
                   description: "last time this lock was taken or attemped to take"
                },
                created: {
                   bsonType: "date",
                   description: "creation date of the lock"
                }
            }
        }
    }
});
print(log);

print("Creating collection index unique name ");
log = db.getCollection("locks").createIndex( { "name": 1 }, { unique: true } );
print(log);

print("Creating collection index expirable expiredAt ");
log = db.getCollection("locks").createIndex( { "expireAt": 1 }, { expireAfterSeconds: 0 } )
print(log);



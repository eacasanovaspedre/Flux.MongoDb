namespace Flux.MongoDb

open MongoDB.Driver
open System.Linq
open MongoDB.Bson
open Flux.LanguageUtils.Linq.Expressions
open Flux

[<Struct>]
type ConnectionString = ConnectionString of NEString

[<Struct>]
type Client = Client of MongoClient

[<Struct>]
type Session =
    | Session of IClientSessionHandle
    interface System.IDisposable with
        member this.Dispose(): unit =
            match this with
            | Session session -> session.Dispose()


[<Struct>]
type Database = Database of MongoDB.Driver.IMongoDatabase

[<Struct>]
type 'T Collection = Collection of 'T MongoDB.Driver.IMongoCollection

[<Struct>]
type CollectionName = CollectionName of NEString

module ObjectId =

    let inline create() = ObjectId.GenerateNewId()

    let inline asString (objectId: ObjectId) = objectId.ToString()

module ConnectionString =

    let inline create str = ConnectionString str

    let inline rawValue (ConnectionString str) = str

module Client =

    let inline create connectionString =
        connectionString
        |> ConnectionString.rawValue
        |> NEString.stringValue
        |> MongoClient
        |> Client

    let inline mongoClient (Client client) = client

    let inline internal getMongoDatabase client = (client |> mongoClient).GetDatabase

module Session =

    let inline start client = (Client.mongoClient client).StartSession() |> Session

    let inline getMongoSession (Session session) = session

module Transaction =

    let inline start (Session session) = session.StartTransaction()

    let inline abort (Session session) = session.AbortTransaction()

    let inline commit (Session session) = session.CommitTransaction()

module Database =

    let inline get dbName client =
        dbName
        |> NEString.stringValue
        |> Client.getMongoDatabase client
        |> Database

    let inline internal getMongoDatabase (Database db) = db

    let inline internal getMongoCollection database = (getMongoDatabase database).GetCollection<_>

module UpdateDefinition =

    let field (selector: Selector<_, _>) value = Builders<_>.Update.Set(selector, value)

    let combine (selectors: _ seq) = Builders<_>.Update.Combine selectors

module Collection =

    let inline get collectionName database =
        collectionName
        |> NEString.stringValue
        |> Database.getMongoCollection database
        |> Collection

    let inline query (Collection collection) = collection.AsQueryable()

    let createDocumentWithObjectId createWithId =
        ()
        |> ObjectId.create
        |> ObjectId.asString
        |> createWithId

    let insertOne (Collection collection) doc = collection.InsertOne doc

    let insertOne' (Collection collection) (Session session) doc = collection.InsertOne(session, doc, null)

    let insertMany (Collection collection) docs = collection.InsertMany(docs)

    let insertMany' (Collection collection) (Session session) docs = collection.InsertMany(session, docs, null)

    let deleteOne (Collection collection) (filter: _ Pred) = collection.DeleteOne(filter)

    let deleteOne' (Collection collection) (filter: _ Pred) (Session session) = collection.DeleteOne(session, filter)

    let deleteMany (Collection collection) (filter: _ Pred) = collection.DeleteMany(filter)

    let deleteMany' (Collection collection) (filter: _ Pred) (Session session) = collection.DeleteMany(session, filter)

    let updateField (Collection collection) (selector: Selector<_, _>) (filter: _ Pred) value =
        let update = UpdateDefinition.field selector value
        collection.UpdateOne(filter, update)

    let updateField' (Collection collection) (selector: Selector<_, _>) (Session session) (filter: _ Pred) value =
        let update = UpdateDefinition.field selector value
        collection.UpdateOne(session, filter, update)

    let updateFields (Collection collection) (filter: _ Pred) (updateDefinitions: UpdateDefinition<_> seq) =
        let update = UpdateDefinition.combine updateDefinitions
        collection.UpdateOne(filter, update)

    let updateFields'
        (Collection collection)
        (Session session)
        (filter: _ Pred)
        (updateDefinitions: UpdateDefinition<_> seq)
        =
        let update = Builders<_>.Update.Combine updateDefinitions
        collection.UpdateOne(session, filter, update)

    let replaceOne (Collection collection) (filter: _ Pred) isUpsert object =
        collection.ReplaceOne(filter, object, ReplaceOptions(IsUpsert = isUpsert))

    let replaceOne' (Collection collection) (Session session) (filter: _ Pred) isUpsert object =
        collection.ReplaceOne(session, filter, object, ReplaceOptions(IsUpsert = isUpsert))

    let inline unwind (selector: Selector<_, _>) (query: _ IQueryable) = selector |> query.SelectMany

    let inline nullToOption k =
        if obj.ReferenceEquals(k, null) then None else Some k

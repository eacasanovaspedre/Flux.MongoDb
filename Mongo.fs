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

module Collection =

    let inline get collectionName database =
        collectionName
        |> NEString.stringValue
        |> Database.getMongoCollection database
        |> Collection

    let inline query (Collection collection) = collection.AsQueryable()

    let createDocument createWithId =
        ()
        |> ObjectId.create
        |> ObjectId.asString
        |> createWithId

    let insertOne (Collection collection) session doc =
        match session with
        | None -> collection.InsertOne doc
        | Some(Session session) -> collection.InsertOne(session, doc)

    let insertMany (Collection collection) session docs =
        match session with
        | None -> collection.InsertMany(docs)
        | Some(Session session) -> collection.InsertMany(session, docs)

    let insertOneAdHoc collection session createWithId = insertOne collection session (createDocument createWithId)

    let deleteOne (filter: _ Pred) (Collection collection) session =
        match session with
        | None -> collection.DeleteOne(filter)
        | Some(Session session) -> collection.DeleteOne(session, filter)

    let deleteMany (filter: _ Pred) (Collection collection) session =
        match session with
        | None -> collection.DeleteMany(filter)
        | Some(Session session) -> collection.DeleteMany(session, filter)

    let updateField (filter: _ Pred) (selector: Selector<_, _>) value ((Collection collection) as col) =
        let update = Builders<_>.Update.Set(selector, value)
        col, collection.UpdateOne(filter, update)

    let updateFieldDefinition (selector: Selector<_, _>) value = Builders<_>.Update.Set(selector, value)

    let updateCombine ((Collection collection) as col) (filter: _ Pred) (updateDefinitions: UpdateDefinition<_> seq) =
        let update = Builders<_>.Update.Combine updateDefinitions
        col, collection.UpdateOne(filter, update)

    let replaceOne (filter: _ Pred) object ((Collection collection) as col) = col, collection.ReplaceOne(filter, object)

    let replaceOneAsync (filter: _ Pred) object ((Collection collection) as col) =
        async {
            let! result = collection.ReplaceOneAsync(filter, object) |> Async.AwaitTask
            return col, result }

    let inline unwind (selector: Selector<_, _>) (query: _ IQueryable) = selector |> query.SelectMany

    let inline nullToOption k =
        if obj.ReferenceEquals(k, null) then None else Some k

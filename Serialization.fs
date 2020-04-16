module Flux.MongoDb.Serialization

open MongoDB.Bson.Serialization.Serializers
open Flux
open MongoDB.Bson.Serialization
open MongoDB.Bson
open System
open FSharp.Reflection

type EmailSerializer() =
    inherit SerializerBase<Email>()

    override __.Serialize(context, _, value) =
        value
        |> Email.stringValue
        |> context.Writer.WriteString

    override this.Deserialize(context, _) =
        let reader = context.Reader
        match ()
              |> reader.ReadString
              |> Email.tryCreate with
        | Ok email -> email
        | Error _ -> raise (this.CreateCannotBeDeserializedException())

    static member Register() = MongoDB.Bson.Serialization.BsonSerializer.RegisterSerializer<Email>(EmailSerializer())

type NEStringSerializer() =
    inherit SerializerBase<NEString>()

    override __.Serialize(context, _, value) =
        value
        |> NEString.stringValue
        |> context.Writer.WriteString

    override this.Deserialize(context, _) =
        let reader = context.Reader
        match ()
              |> reader.ReadString
              |> NEString.tryCreate with
        | Ok neStr -> neStr
        | Error _ -> raise (this.CreateCannotBeDeserializedException())

    static member Register() =
        MongoDB.Bson.Serialization.BsonSerializer.RegisterSerializer<NEString>(NEStringSerializer())

type ListSerializer<'T>() =
    inherit SerializerBase<'T list>()

    override __.Serialize(context, _, value) =
        let writer = context.Writer
        writer.WriteStartArray()
        value |> List.iter (fun x -> BsonSerializer.Serialize(writer, typeof<'T>, x))
        writer.WriteEndArray()

    override __.Deserialize(context, _) =
        let reader = context.Reader

        match reader.CurrentBsonType with
        | BsonType.Null ->
            reader.ReadNull()
            []
        | BsonType.Array ->
            seq {
                reader.ReadStartArray()
                while reader.ReadBsonType() <> BsonType.EndOfDocument do
                    yield BsonSerializer.Deserialize<'T>(reader)
                reader.ReadEndArray()
            }
            |> Seq.toList
        | bsonType ->
            sprintf "Can't deserialize a %s from BsonType %s" typeof<'T list>.FullName (bsonType.ToString())
            |> InvalidOperationException
            |> raise

    interface IBsonArraySerializer with
        member __.TryGetItemSerializationInfo serializationInfo =
            let nominalType = typeof<'T>
            serializationInfo <- BsonSerializationInfo(null, BsonSerializer.LookupSerializer<'T>(), nominalType)
            true

    static member Register() =
        BsonSerializer.RegisterGenericSerializerDefinition(typeof<list<_>>, typeof<ListSerializer<_>>)

module Conventions =

    let isId str = str = "_id" || str.ToLower() = "id"

    let shouldMapStringIdToObjectId (m: BsonMemberMap) =
        m.MemberName.EndsWith "Id" && (m.MemberType.Equals typeof<string> || m.MemberType.Equals typeof<NEString>)

let private memberName (property: Reflection.PropertyInfo) = property.Name

let registerSerializers() =
    ListSerializer<_>.Register()
    EmailSerializer.Register()
    NEStringSerializer.Register()

let registerClassMapForRecord<'T> idAsObjectId isId shouldMapStringIdToObjectId postConfig =
    let ctor = FSharpValue.PreComputeRecordConstructorInfo(typeof<'T>)
    let fields = FSharpType.GetRecordFields(typeof<'T>)
    let fieldNames = fields |> Array.map (fun f -> f.Name)
    BsonClassMap.RegisterClassMap<'T>
        (Action<_>(fun cm ->
            cm.MapConstructor(ctor, fieldNames) |> ignore
            let (ids, properties) =
                fields |> Array.partition (memberName >> (Option.defaultValue Conventions.isId isId))
            ids
            |> Array.head
            |> cm.MapIdMember
            |> ignore
            properties
            |> Array.iter
                (cm.MapMember
                 >> (fun m ->
                     match Option.bind (fun mapper -> mapper m) shouldMapStringIdToObjectId with
                     | Some true -> m.SetSerializer(StringSerializer(BsonType.ObjectId)) |> ignore
                     | None when Conventions.shouldMapStringIdToObjectId m ->
                         m.SetSerializer(StringSerializer(BsonType.ObjectId)) |> ignore
                     | Some false -> ()
                     | None -> ()))
            do if idAsObjectId then cm.IdMemberMap.SetSerializer(StringSerializer(BsonType.ObjectId)) |> ignore
            postConfig cm))
    |> ignore

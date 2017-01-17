using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using MongoDB.Driver.Core;
using MongoDB.Driver.GeoJsonObjectModel;
using MongoDB.Driver.GridFS;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;

namespace ConsoleApplication
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting");

            dbConn();

            Console.WriteLine("Finished");

        }

        public static void dbConn()
        {
            // To directly connect to a single MongoDB server
            // (this will not auto-discover the primary even if it's a member of a replica set)
            var client = new MongoDB.Driver.MongoClient();

            // or use a connection string
//            var client = new MongoClient("mongodb://localhost:27017");

            // or, to connect to a replica set, with auto-discovery of the primary, supply a seed list of members
//            var client = new MongoClient("mongodb://localhost:27017,localhost:27018,localhost:27019");

            // Get database
            var database = client.GetDatabase("test");
            // Get database document collection 
            var collection = database.GetCollection<BsonDocument>("restaurants");

            // Count all docuemts in the collection
            var countall = collection.Count(new BsonDocument());
            // Output document count
            Console.WriteLine("All Document Count: " + countall);

            // Find All Documents in a Collection
            var documents = collection.Find(new BsonDocument()).ToList();

            // To iterate over the returned documents using the synchronous API
            var mycursor = collection.Find(new BsonDocument()).ToCursor();
            foreach (var document in mycursor.ToEnumerable())
            {
                // Do something...
            }

            // Projection.  Exclude the “_id” field and output the first matching document:
            var projection = Builders<BsonDocument>.Projection.Exclude("_id");
            var documentfoo = collection.Find(new BsonDocument()).Project(projection).First();
            Console.WriteLine(documentfoo.ToString());


            // Get the document where the address building coordinate is -73.856076999999999, 40.848447
            var filterone = Builders<BsonDocument>.Filter.Eq("address.street", "Morris Park Ave");
            var document1 = collection.Find(filterone).First();
            Console.WriteLine(document1);

            // Update a document
            var filtertwo = Builders<BsonDocument>.Filter.Eq("address.street", "Morris Park Ave") & Builders<BsonDocument>.Filter.Eq("grades.grade", "A");
            var update = Builders<BsonDocument>.Update.Set("grades.grade", "F");

            // Query first bson document in the collection
            var adocument = collection.Find(new BsonDocument()).FirstOrDefault();
            // Console.WriteLine(adocument.ToString());
            // Output the bson document as json
            Console.WriteLine(adocument.ToJson());



            // Query all documents in collection
            var filter = new BsonDocument();
            var count = 0;
            using (var cursor = collection.FindSync<BsonDocument>(filter))
            {
                while (cursor.MoveNext())
                {
                    var batch = cursor.Current;
                    foreach (var document in batch)
                    {
                        // process document
                        count++;
                    }
                }
            }

            // Output document count
            Console.WriteLine("Document Count: " + count);

            // The following operation finds documents whose borough field equals "Manhattan".
            var afilter = MongoDB.Driver.Builders<BsonDocument>.Filter.Eq("borough", "Manhattan");
            count = 0;
            using (var cursor = collection.FindSync<BsonDocument>(afilter))
            {
                while (cursor.MoveNext())
                {
                    var batch = cursor.Current;
                    foreach (var document in batch)
                    {
                        // process document
                        count++;
                    }
                }
            }

            // Output document count
            Console.WriteLine("Document Count: " + count);

            var filter1 = Builders<BsonDocument>.Filter.Eq("borough", "Manhattan");
            var result1 = collection.Find(filter1).ToListAsync();
            // Output documents in bson
            Console.WriteLine(result1.ToBsonDocument());

            //  The following operation specifies an equality condition on the zipcode field in the address embedded document.            
            var bfilter = MongoDB.Driver.Builders<BsonDocument>.Filter.Eq("address.zipcode", "10075");
            count = 0;
            using (var cursor = collection.FindSync<BsonDocument>(bfilter))
            {
                while (cursor.MoveNext())
                {
                    var batch = cursor.Current;
                    foreach (var document in batch)
                    {
                        // process document
                        count++;
                    }
                }
            }

            // Output document count
            Console.WriteLine("Document Count: " + count);

            // Query for documents whose grades array contains an embedded document with a field score greater than 30
            var cfilter = MongoDB.Driver.Builders<BsonDocument>.Filter.Gt("grades.score", 30);
            count = 0;
            using (var cursor = collection.FindSync<BsonDocument>(cfilter))
            {
                while (cursor.MoveNext())
                {
                    var batch = cursor.Current;
                    foreach (var document in batch)
                    {
                        // process document
                        count++;
                    }
                }
            }

            // Output document count
            Console.WriteLine("Document Count: " + count);

            // Query for documents whose grades array contains an embedded document with a field score less than 10.
            var dfilter = MongoDB.Driver.Builders<BsonDocument>.Filter.Lt("grades.score", 10);
            count = 0;
            using (var cursor = collection.FindSync<BsonDocument>(dfilter))
            {
                while (cursor.MoveNext())
                {
                    var batch = cursor.Current;
                    foreach (var document in batch)
                    {
                        // process document
                        count++;
                    }
                }
            }

            // Output document count
            Console.WriteLine("Document Count: " + count);

            // Specify a logical conjunction (AND) for a list of query conditions by joining the conditions with an ampersand 
            var builder = MongoDB.Driver.Builders<BsonDocument>.Filter;
            var efilter = builder.Eq("cuisine", "Italian") & builder.Eq("address.zipcode", "10075");
            count = 0;
            using (var cursor = collection.FindSync<BsonDocument>(efilter))
            {
                while (cursor.MoveNext())
                {
                    var batch = cursor.Current;
                    foreach (var document in batch)
                    {
                        // process document
                        count++;
                    }
                }
            }

            // Output document count
            Console.WriteLine("Document Count: " + count);

            // Specify a logical disjunction (OR) for a list of query conditions by joining the conditions with a pipe
            var abuilder = MongoDB.Driver.Builders<BsonDocument>.Filter;
            var ffilter = abuilder.Eq("cuisine", "Italian") | abuilder.Eq("address.zipcode", "10075");;
            count = 0;
            using (var cursor = collection.FindSync<BsonDocument>(ffilter))
            {
                while (cursor.MoveNext())
                {
                    var batch = cursor.Current;
                    foreach (var document in batch)
                    {
                        // process document
                        count++;
                    }
                }
            }

            // Output document count
            Console.WriteLine("Document Count: " + count);

            // Sorting.   the following operation returns all documents in the restaurants collection, sorted first by the borough field in ascending order, and then, within each borough, by the "address.zipcode" field in ascending order
            var acollection = database.GetCollection<BsonDocument>("restaurants");
            var gfilter = new BsonDocument();
            var sort = Builders<BsonDocument>.Sort.Ascending("borough").Ascending("address.zipcode");
            var result = collection.Find(gfilter).Sort(sort).ToListAsync();
            // Output documents in json
            Console.WriteLine(result.ToJson());
        }
    }
}

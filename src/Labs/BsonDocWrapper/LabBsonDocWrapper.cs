using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Driver;
using Xunit;
using Xunit.Abstractions;

namespace Labs.BsonDocWrapper
{
    namespace Dennis
    {
        public class UnknownA
        {
            public string Value { get; set; }
        }
    }

    namespace George
    {
        public class UnknownA
        {
            public string Value { get; set; }
        }
    }

    public class LabDoc
    {
        [BsonId(IdGenerator = typeof(AscendingGuidGenerator))]
        [BsonRepresentation(BsonType.String)]
        public Guid Id { get; set; }

        public List<BsonDocument> Contents { get; set; }
    }

    public class LabBsonDocWrapperFixture : DbFixture
    {
        public IMongoCollection<LabDoc> Collection { get; }

        public LabBsonDocWrapperFixture(IMessageSink sink) : base(sink)
        {
            Collection = Db.GetCollection<LabDoc>("bsondocwrapper");
            Collection.DeleteMany(FilterDefinition<LabDoc>.Empty);
        }
    }

    public class BsonDocWrapper : IClassFixture<LabBsonDocWrapperFixture>
    {
        private readonly LabBsonDocWrapperFixture _fixture;

        public BsonDocWrapper(LabBsonDocWrapperFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task Can_be_used_to_store_and_return_untyped_documents()
        {
            var orgDoc = new LabDoc
            {
                Contents = new List<BsonDocument>
                {
                    BsonDocumentWrapper.Create(new Dennis.UnknownA { Value = "UnknownA from Dennis" })
                }
            };

            await _fixture.Collection.InsertOneAsync(orgDoc);

            var returnedDoc = await _fixture.Collection.Find(d => d.Id == orgDoc.Id).SingleAsync();
            returnedDoc.Should().BeEquivalentTo(orgDoc);

            var dennisBsonDoc = returnedDoc.Contents.First();
            var dennis = BsonSerializer.Deserialize<Dennis.UnknownA>(dennisBsonDoc);
            var dennisAsGeorge = BsonSerializer.Deserialize<George.UnknownA>(dennisBsonDoc);
            dennis.Value.Should().Be(dennisAsGeorge.Value);
        }
    }
}

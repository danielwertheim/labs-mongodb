using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Driver;
using Xunit;
using Xunit.Abstractions;

namespace Labs.ObjectDiscriminator
{
    namespace Dennis
    {
        public class UnknownA
        {
            public string Value { get; set; }
        }

        public class UnknownB
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

        public class UnknownB
        {
            public string Value { get; set; }
        }
    }

    public class LabDoc
    {
        [BsonId(IdGenerator = typeof(AscendingGuidGenerator))]
        [BsonRepresentation(BsonType.String)]
        public Guid Id { get; set; }

        public List<object> Content { get; set; }
    }

    public class LabObjectDiscriminatorFixture : DbFixture
    {
        public IMongoCollection<LabDoc> Collection { get; }

        public LabObjectDiscriminatorFixture(IMessageSink sink) : base(sink)
        {
            Collection = Db.GetCollection<LabDoc>("objectdiscriminator");
            Collection.DeleteMany(FilterDefinition<LabDoc>.Empty);
        }
    }

    public class ObjectDiscriminator : IClassFixture<LabObjectDiscriminatorFixture>
    {
        private readonly LabObjectDiscriminatorFixture _fixture;

        public ObjectDiscriminator(LabObjectDiscriminatorFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task Can_not_rehydrate_When_discriminator_matches_type_from_different_namespaces()
        {
            var orgDoc = new LabDoc
            {
                Content = new List<object>
                {
                    new Dennis.UnknownA { Value = "UnknownA from Dennis" },
                    new George.UnknownA { Value = "UnknownA from George" }
                }
            };

            await _fixture.Collection.InsertOneAsync(orgDoc);

            (await FluentActions
                .Invoking(async () => await _fixture.Collection.Find(d => d.Id == orgDoc.Id).SingleAsync())
                .Should().ThrowAsync<FormatException>())
                .And.InnerException
                .Should().BeOfType<BsonSerializationException>();
        }

        [Fact]
        public async Task Can_rehydrate_When_discriminator_uses_full_type_name()
        {
            BsonSerializer.RegisterDiscriminatorConvention(typeof(Dennis.UnknownB), ObjectDiscriminatorConvention.Instance);
            BsonSerializer.RegisterDiscriminatorConvention(typeof(George.UnknownB), ObjectDiscriminatorConvention.Instance);

            var orgDoc = new LabDoc
            {
                Content = new List<object>
                {
                    new Dennis.UnknownB { Value = "UnknownB from Dennis" },
                    new George.UnknownB { Value = "UnknownB from George" }
                }
            };

            await _fixture.Collection.InsertOneAsync(orgDoc);

            var returnedDoc = await _fixture.Collection.Find(d => d.Id == orgDoc.Id).SingleAsync();

            returnedDoc.Should().BeEquivalentTo(orgDoc);
        }
    }
}

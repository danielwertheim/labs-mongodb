using System;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using Xunit;
using Xunit.Abstractions;

namespace Labs.Guids
{
    /// <summary>
    /// GUIDs tests need to run 'LabV2Guids & LabV3Guids Can_query_using_expression_against_Ids_and_props', ONCE IN ISOLATION.
    /// </summary>
    public class LabGuidsV2 : IClassFixture<LabV2GuidsFixture>
    {
        private readonly LabV2GuidsFixture _fixture;

        public LabGuidsV2(LabV2GuidsFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task Can_query_using_expression_against_Ids_and_props()
        {
            var viaId = await _fixture.CollectionV2.Find(d => d.Id == _fixture.Doc.Id).SingleOrDefaultAsync();
            var viaSample = await _fixture.CollectionV2.Find(d => d.Sample == _fixture.Doc.Sample).SingleOrDefaultAsync();

            viaId.Should().BeEquivalentTo(_fixture.Doc);
            viaSample.Should().BeEquivalentTo(_fixture.Doc);
        }

        [Fact]
        public async Task Is_not_compatible_with_V3_guids_When_it_comes_to_querying()
        {
            var all = _fixture.CollectionV3.Find(_ => true).ToList();
            all.Should().NotBeEmpty("You need to run a V3 test first in isolation to get V3 data.");
            all.Should().Contain(d => d.Id == LabGuidsFixtureBase.IdV3);

            var matchViaExpression = await _fixture.CollectionV3.Find(d => d.Id == LabGuidsFixtureBase.IdV3).SingleOrDefaultAsync();
            matchViaExpression.Should().BeNull("V2 should not have been able to find V3 data.");

            var matchViaFilter = await _fixture.CollectionV3.Find(LabGuidsFixtureBase.DocFilterV3).SingleOrDefaultAsync();
            matchViaFilter.Should().BeNull("V2 should not have been able to find V3 data.");
        }
    }

    /// <summary>
    /// GUIDs tests need to run 'LabV2Guids & LabV3Guids Can_query_using_expression_against_Ids_and_props', ONCE IN ISOLATION.
    /// </summary>
    public class LabGuidsV3 : IClassFixture<LabV3GuidsFixture>
    {
        private readonly LabV3GuidsFixture _fixture;

        public LabGuidsV3(LabV3GuidsFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task Can_query_using_expression_against_Ids_and_props()
        {
            var viaId = await _fixture.CollectionV3.Find(d => d.Id == _fixture.Doc.Id).SingleOrDefaultAsync();
            var viaSample = await _fixture.CollectionV3.Find(d => d.Sample == _fixture.Doc.Sample).SingleOrDefaultAsync();

            viaId.Should().BeEquivalentTo(_fixture.Doc);
            viaSample.Should().BeEquivalentTo(_fixture.Doc);
        }

        [Fact]
        public void Is_not_compatible_with_V2_data_at_all_due_to_serialization_issue()
            => FluentActions
                .Invoking(() => _fixture.CollectionV2.Find(_ => true).ToList())
                .Should().Throw<FormatException>()
                .WithMessage("*cannot deserialize a Guid when GuidRepresentation is Standard and binary sub type is UuidLegacy*");
    }

    public abstract class LabGuidsFixtureBase : DbFixture
    {
        public static readonly Guid IdV2 = new("9a16c46c-53bf-4ff9-a2e8-d7ecabd2f39f");
        public static FilterDefinition<LabDoc> DocFilterV2 { get; } = new ExpressionFilterDefinition<LabDoc>(i => i.Id == IdV2);

        public static readonly Guid IdV3 = new("ede68e15-28f8-45bb-9ac9-ff2d98583c2c");
        public static FilterDefinition<LabDoc> DocFilterV3 { get; } = new ExpressionFilterDefinition<LabDoc>(i => i.Id == IdV3);

        public IMongoCollection<LabDoc> CollectionV2 => Db.GetCollection<LabDoc>("guids");
        public IMongoCollection<LabDoc> CollectionV3 => Db.GetCollection<LabDoc>("guids-v3");
        public LabDoc Doc { get; protected set; }

        protected LabGuidsFixtureBase(IMessageSink sink) : base(sink) { }
    }

    public class LabV2GuidsFixture : LabGuidsFixtureBase
    {
        public LabV2GuidsFixture(IMessageSink sink) : base(sink)
        {
            Doc = CollectionV2.Find(DocFilterV2).SingleOrDefault();
            if (Doc != null)
                return;

            Doc = LabDoc.Create(IdV2);
            CollectionV2.InsertOne(Doc);
        }
    }

    public class LabV3GuidsFixture : LabGuidsFixtureBase
    {
        public LabV3GuidsFixture(IMessageSink sink) : base(sink)
        {
            BsonDefaults.GuidRepresentationMode = GuidRepresentationMode.V3;
            BsonSerializer.RegisterSerializer(new GuidSerializer(GuidRepresentation.Standard));

            Doc = CollectionV3.Find(DocFilterV3).SingleOrDefault();
            if (Doc != null)
                return;

            Doc = LabDoc.Create(IdV3);
            CollectionV3.InsertOne(Doc);
        }
    }

    public class LabDoc
    {
        public Guid Id { get; set; }
        public Guid Sample { get; set; }

        public static LabDoc Create(Guid id)
            => new() {Id = id, Sample = Guid.NewGuid()};
    }
}

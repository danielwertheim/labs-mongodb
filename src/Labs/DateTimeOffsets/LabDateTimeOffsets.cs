using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Driver;
using Xunit;
using Xunit.Abstractions;

namespace Labs.DateTimeOffsets
{
    public class LabDoc
    {
        [BsonId(IdGenerator = typeof(AscendingGuidGenerator))]
        [BsonRepresentation(BsonType.String)]
        public Guid Id { get; set; }

        public DateTimeOffset LocalDateTimeOffset { get; set; }
        public DateTimeOffset UtcDateTimeOffset { get; set; }
    }

    public class LabDateTimeOffsetsFixture : DbFixture
    {
        private static readonly object Sync = new();
        public IMongoCollection<LabDoc> Collection { get; }
        public DateTimeOffset Start { get; }
        public DateTimeOffset StartUtc { get; }

        public LabDateTimeOffsetsFixture(IMessageSink sink) : base(sink)
        {
            Collection = Db.GetCollection<LabDoc>("datetimeoffsets");

            Start = new DateTime(2021, 5, 1, 8, 0, 0);
            StartUtc = Start.ToUniversalTime();
        }

        public void EnsureSampleDateExists()
        {
            lock (Sync)
            {
                var dataExists = Collection.Find(x => true).Any();
                if (dataExists)
                    return;

                var labDocs = Enumerable
                    .Range(0, 2)
                    .SelectMany(day => Enumerable.Range(0, 12)
                        .Select(hour =>
                        {
                            var local = Start.AddDays(day).AddHours(hour);
                            var utc = local.ToUniversalTime();

                            return new LabDoc
                            {
                                LocalDateTimeOffset = local,
                                UtcDateTimeOffset = utc
                            };
                        }));

                Collection.InsertMany(labDocs);
            }
        }
    }

    public class UtcDateTimeOffset : IClassFixture<LabDateTimeOffsetsFixture>
    {
        private readonly LabDateTimeOffsetsFixture _fixture;

        public UtcDateTimeOffset(LabDateTimeOffsetsFixture fixture)
        {
            _fixture = fixture;
            fixture.EnsureSampleDateExists();
        }

        [Fact]
        public async Task A_Queries_Should_match_utc_date_arg()
            => (await _fixture.Collection
                    .Find(d => d.UtcDateTimeOffset <= _fixture.StartUtc.AddHours(3))
                    .ToListAsync())
                .Select(d => d.UtcDateTimeOffset)
                .Should().HaveCount(4)
                .And.ContainInOrder(_fixture.StartUtc, _fixture.StartUtc.AddHours(1), _fixture.StartUtc.AddHours(2), _fixture.StartUtc.AddHours(3));

        [Fact]
        public async Task B_Queries_Should_not_match_when_millisecond_differs()
            => (await _fixture.Collection
                .Find(d => d.UtcDateTimeOffset == _fixture.StartUtc.AddMilliseconds(-1))
                .AnyAsync()).Should().BeFalse();
    }

    public class LocalDateTimeOffset : IClassFixture<LabDateTimeOffsetsFixture>
    {
        private readonly LabDateTimeOffsetsFixture _fixture;

        public LocalDateTimeOffset(LabDateTimeOffsetsFixture fixture)
        {
            _fixture = fixture;
            fixture.EnsureSampleDateExists();
        }

        [Fact]
        public async Task A_Queries_Should_match_local_date_arg()
            => (await _fixture.Collection
                    .Find(d => d.LocalDateTimeOffset <= _fixture.Start.AddHours(3))
                    .ToListAsync())
                .Select(d => d.LocalDateTimeOffset)
                .Should().HaveCount(4)
                .And.ContainInOrder(_fixture.Start, _fixture.Start.AddHours(1), _fixture.Start.AddHours(2), _fixture.Start.AddHours(3));

        [Fact]
        public async Task B_Queries_Should_not_match_when_millisecond_differs()
            => (await _fixture.Collection
                .Find(d => d.LocalDateTimeOffset == _fixture.Start.AddMilliseconds(-1))
                .AnyAsync()).Should().BeFalse();
    }
}

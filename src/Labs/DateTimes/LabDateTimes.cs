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

namespace Labs.DateTimes
{
    public class LabDoc
    {
        [BsonId(IdGenerator = typeof(AscendingGuidGenerator))]
        [BsonRepresentation(BsonType.String)]
        public Guid Id { get; set; }

        public DateTime LocalDateTimeWithoutKind { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public DateTime LocalDateTimeWithKind { get; set; }

        public DateTime UtcDateTime { get; set; }

        public DateTimeOffset LocalDateTimeOffset { get; set; }
        public DateTimeOffset UtcDateTimeOffset { get; set; }
    }
    
    public class LabDateTimesFixture : DbFixture
    {
        public IMongoCollection<LabDoc> Collection { get; }
        public DateTime Start { get; }
        public DateTime StartUtc { get; }

        public LabDateTimesFixture(IMessageSink sink) : base(sink)
        {
            Collection = Db.GetCollection<LabDoc>("datetimes");
            
            Start = new DateTime(2021, 5, 1, 8, 0, 0);
            StartUtc = Start.ToUniversalTime();
        }
        
        public void EnsureSampleDateExists()
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
                            LocalDateTimeWithoutKind = local,
                            LocalDateTimeWithKind = local,
                            UtcDateTime = utc,
                            LocalDateTimeOffset = local,
                            UtcDateTimeOffset = utc
                        };
                    }));

            Collection.InsertMany(labDocs);
        }
    }

    public class UtcDate : IClassFixture<LabDateTimesFixture>
    {
        private readonly LabDateTimesFixture _fixture;

        public UtcDate(LabDateTimesFixture fixture)
        {
            _fixture = fixture;
            fixture.EnsureSampleDateExists();
        }

        [Fact]
        public async Task A_Returned_date_time_Should_have_kind_Utc()
            => (await _fixture.Collection.Find(d => true).FirstAsync())
                .UtcDateTime.Kind
                .Should().Be(DateTimeKind.Utc);
        
        [Fact]
        public async Task B_Queries_Should_match_utc_date_arg()
            => (await _fixture.Collection
                    .Find(d => d.UtcDateTime <= _fixture.StartUtc.AddHours(3))
                    .ToListAsync())
                .Select(d => d.UtcDateTime)
                .Should().HaveCount(4)
                .And.ContainInOrder(_fixture.StartUtc, _fixture.StartUtc.AddHours(1), _fixture.StartUtc.AddHours(2), _fixture.StartUtc.AddHours(3));

        [Fact]
        public async Task C_Queries_Should_not_match_when_millisecond_differs()
            => (await _fixture.Collection
                .Find(d => d.UtcDateTime == _fixture.StartUtc.AddMilliseconds(-1))
                .AnyAsync()).Should().BeFalse();
    }
    
    public class LocalDateWithoutKind : IClassFixture<LabDateTimesFixture>
    {
        private readonly LabDateTimesFixture _fixture;

        public LocalDateWithoutKind(LabDateTimesFixture fixture)
        {
            _fixture = fixture;
            fixture.EnsureSampleDateExists();
        }

        [Fact]
        public async Task A_Returned_date_time_Should_have_kind_Local()
            => (await _fixture.Collection.Find(d => true).FirstAsync())
                .LocalDateTimeWithoutKind.Kind
                .Should().Be(DateTimeKind.Local);

        [Fact]
        public async Task B_Queries_Should_match_local_date_arg()
            => (await _fixture.Collection
                    .Find(d => d.LocalDateTimeWithoutKind <= _fixture.Start.AddHours(3))
                    .ToListAsync())
                .Select(d => d.LocalDateTimeWithoutKind)
                .Should().HaveCount(4)
                .And.ContainInOrder(_fixture.Start, _fixture.Start.AddHours(1), _fixture.Start.AddHours(2), _fixture.Start.AddHours(3));

        [Fact]
        public async Task C_Queries_Should_not_match_when_millisecond_differs()
            => (await _fixture.Collection
                .Find(d => d.LocalDateTimeWithoutKind == _fixture.Start.AddMilliseconds(-1))
                .AnyAsync()).Should().BeFalse();
    }
    
    public class LocalDateWithKind : IClassFixture<LabDateTimesFixture>
    {
        private readonly LabDateTimesFixture _fixture;

        public LocalDateWithKind(LabDateTimesFixture fixture)
        {
            _fixture = fixture;
            fixture.EnsureSampleDateExists();
        }

        [Fact]
        public async Task A_Returned_date_time_Should_have_kind_Local()
            => (await _fixture.Collection.Find(d => true).FirstAsync())
                .LocalDateTimeWithKind.Kind
                .Should().Be(DateTimeKind.Local);

        [Fact]
        public async Task B_Queries_Should_match_local_date_arg()
            => (await _fixture.Collection
                    .Find(d => d.LocalDateTimeWithKind <= _fixture.Start.AddHours(3))
                    .ToListAsync())
                .Select(d => d.LocalDateTimeWithKind)
                .Should().HaveCount(4)
                .And.ContainInOrder(_fixture.Start, _fixture.Start.AddHours(1), _fixture.Start.AddHours(2), _fixture.Start.AddHours(3));

        [Fact]
        public async Task C_Queries_Should_not_match_when_millisecond_differs()
            => (await _fixture.Collection
                .Find(d => d.LocalDateTimeWithKind == _fixture.Start.AddMilliseconds(-1))
                .AnyAsync()).Should().BeFalse();
    }
}
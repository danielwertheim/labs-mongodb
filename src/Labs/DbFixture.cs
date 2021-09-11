using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Labs
{
    public abstract class DbFixture
    {
        private const string CnString = "mongodb://localhost:27017";
        private const string DbName = "labs";

        protected IMongoClient Client { get; }
        protected IMongoDatabase Db { get; }

        protected DbFixture(IMessageSink sink)
        {
            var fixtureType = GetType().Name;
            var mongoClientSettings = MongoClientSettings.FromConnectionString(CnString);
            mongoClientSettings.ClusterConfigurator = cb 
                => cb.Subscribe<CommandStartedEvent>(e 
                    => sink.OnMessage(new DiagnosticMessage($"'{fixtureType}' - {e.Command.ToJson()}")));

            Client = new MongoClient(mongoClientSettings);
            Db = Client.GetDatabase(DbName);
        }
    }
}
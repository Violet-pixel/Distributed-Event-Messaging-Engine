using Brado.Broker;
using Brado.Domain;
using Brado.EventStore.Dialects;
using Brado.EventStore.Journal;
using Brado.EventStore.Outbox;
using Brado.EventStore.Repositories;
using Brado.EventStore.Sagas;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Brado.EventStore;

public static class EventStoreServiceCollectionExtensions
{
    public static IServiceCollection AddBradoEventStore(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<EventStoreOptions>(configuration.GetSection(EventStoreOptions.Section));

        services.AddSingleton(sp => SqlDialectFactory.Create(sp.GetRequiredService<IOptions<EventStoreOptions>>().Value));
        services.AddSingleton(new EventTypeRegistry().RegisterAssembly(typeof(Order).Assembly));
        services.AddSingleton<EventAppendedSignal>();
        services.AddSingleton<IEventStore, SqlEventStore>();
        services.AddSingleton<IOutboxStore, SqlOutboxStore>();
        services.AddSingleton<ISagaStore, SqlSagaStore>();
        services.AddSingleton<IInboxStore, SqlInboxStore>();
        services.AddSingleton<IMessageJournal, SqlMessageJournal>();
        services.AddTransient(typeof(IAggregateRepository<>), typeof(EventSourcedRepository<>));
        return services;
    }

    public static async Task EnsureEventStoreSchemaAsync(this IServiceProvider services, CancellationToken cancellationToken = default)
    {
        var dialect = services.GetRequiredService<SqlDialect>();
        await EventStoreSchema.EnsureCreatedAsync(dialect, cancellationToken);
    }
}

public static class EventStoreSchema
{
    public static async Task EnsureCreatedAsync(SqlDialect dialect, CancellationToken cancellationToken = default)
    {
        await using var connection = await dialect.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = dialect.SchemaScript;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}

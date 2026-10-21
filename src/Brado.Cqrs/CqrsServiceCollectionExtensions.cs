using System.Data.Common;
using Brado.Cqrs.Projections;
using Brado.Cqrs.ReadModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Brado.Cqrs;

public static class CqrsServiceCollectionExtensions
{
    public static IServiceCollection AddBradoCqrs(this IServiceCollection services, IConfiguration configuration)
    {
        var options = configuration.GetSection(ReadModelOptions.Section).Get<ReadModelOptions>() ?? new ReadModelOptions();
        services.Configure<ReadModelOptions>(configuration.GetSection(ReadModelOptions.Section));

        services.AddDbContext<ReadModelDbContext>(db =>
        {
            switch (options.Provider.Trim().ToLowerInvariant())
            {
                case "postgres" or "postgresql" or "npgsql":
                    db.UseNpgsql(options.ConnectionString);
                    break;
                case "sqlite":
                    EnsureSqliteDirectory(options.ConnectionString);
                    db.UseSqlite(options.ConnectionString);
                    break;
                default:
                    throw new NotSupportedException($"Provider de leitura desconhecido: '{options.Provider}'.");
            }
        });

        services.AddScoped<ICommandDispatcher, CommandDispatcher>();
        services.AddScoped<IQueryDispatcher, QueryDispatcher>();
        RegisterHandlers(services, typeof(ICommandHandler<,>));
        RegisterHandlers(services, typeof(IQueryHandler<,>));

        services.AddSingleton<IProjection, OrderSummaryProjection>();
        services.AddSingleton<IProjection, DailySalesProjection>();
        services.AddSingleton<ProjectionEngine>();
        return services;
    }

    public static async Task EnsureReadModelAsync(this IServiceProvider services, CancellationToken cancellationToken = default)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<ReadModelDbContext>();
        var creator = db.Database.GetService<IRelationalDatabaseCreator>();
        if (!await creator.ExistsAsync(cancellationToken)) await creator.CreateAsync(cancellationToken);
        if (await db.Database.CanConnectAsync(cancellationToken) && await TablesExistAsync(db, cancellationToken)) return;
        await creator.CreateTablesAsync(cancellationToken);
    }

    private static async Task<bool> TablesExistAsync(ReadModelDbContext db, CancellationToken cancellationToken)
    {
        try
        {
            await db.Checkpoints.AsNoTracking().AnyAsync(cancellationToken);
            return true;
        }
        catch (DbException)
        {
            return false;
        }
    }

    private static void RegisterHandlers(IServiceCollection services, Type openInterface)
    {
        var handlers = typeof(CqrsServiceCollectionExtensions).Assembly.GetTypes()
            .Where(t => t is { IsAbstract: false, IsInterface: false, IsGenericTypeDefinition: false });

        foreach (var type in handlers)
        foreach (var contract in type.GetInterfaces().Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == openInterface))
            services.AddScoped(contract, type);
    }

    private static void EnsureSqliteDirectory(string connectionString)
    {
        var builder = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder(connectionString);
        if (string.IsNullOrWhiteSpace(builder.DataSource) || builder.DataSource == ":memory:") return;
        var directory = Path.GetDirectoryName(Path.GetFullPath(builder.DataSource));
        if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
    }
}

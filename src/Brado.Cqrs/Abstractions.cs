using System.Collections.Concurrent;
using Brado.EventStore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Brado.Cqrs;

public interface ICommand<TResult>
{
}

public interface IQuery<TResult>
{
}

public interface ICommandHandler<in TCommand, TResult> where TCommand : ICommand<TResult>
{
    Task<TResult> HandleAsync(TCommand command, CancellationToken cancellationToken);
}

public interface IQueryHandler<in TQuery, TResult> where TQuery : IQuery<TResult>
{
    Task<TResult> HandleAsync(TQuery query, CancellationToken cancellationToken);
}

public interface ICommandDispatcher
{
    Task<TResult> SendAsync<TResult>(ICommand<TResult> command, CancellationToken cancellationToken = default);
}

public interface IQueryDispatcher
{
    Task<TResult> QueryAsync<TResult>(IQuery<TResult> query, CancellationToken cancellationToken = default);
}

public sealed class CommandDispatcher(IServiceProvider services, ILogger<CommandDispatcher> logger) : ICommandDispatcher
{
    private const int MaxConcurrencyRetries = 3;
    private static readonly ConcurrentDictionary<(Type, Type), object> Wrappers = new();

    public async Task<TResult> SendAsync<TResult>(ICommand<TResult> command, CancellationToken cancellationToken = default)
    {
        var wrapper = (HandlerWrapper<TResult>)Wrappers.GetOrAdd((command.GetType(), typeof(TResult)),
            static key => Activator.CreateInstance(typeof(CommandWrapper<,>).MakeGenericType(key.Item1, key.Item2))!);

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await wrapper.HandleAsync(command, services, cancellationToken);
            }
            catch (ConcurrencyException ex) when (attempt < MaxConcurrencyRetries)
            {
                logger.LogDebug("Conflito em {Stream}; tentativa {Attempt} de {Max}", ex.StreamId, attempt + 1, MaxConcurrencyRetries);
                await Task.Delay(Random.Shared.Next(5, 25) * attempt, cancellationToken);
            }
        }
    }
}

public sealed class QueryDispatcher(IServiceProvider services) : IQueryDispatcher
{
    private static readonly ConcurrentDictionary<(Type, Type), object> Wrappers = new();

    public Task<TResult> QueryAsync<TResult>(IQuery<TResult> query, CancellationToken cancellationToken = default)
    {
        var wrapper = (HandlerWrapper<TResult>)Wrappers.GetOrAdd((query.GetType(), typeof(TResult)),
            static key => Activator.CreateInstance(typeof(QueryWrapper<,>).MakeGenericType(key.Item1, key.Item2))!);
        return wrapper.HandleAsync(query, services, cancellationToken);
    }
}

internal abstract class HandlerWrapper<TResult>
{
    public abstract Task<TResult> HandleAsync(object request, IServiceProvider services, CancellationToken cancellationToken);
}

internal sealed class CommandWrapper<TCommand, TResult> : HandlerWrapper<TResult> where TCommand : ICommand<TResult>
{
    public override Task<TResult> HandleAsync(object request, IServiceProvider services, CancellationToken cancellationToken) =>
        services.GetRequiredService<ICommandHandler<TCommand, TResult>>().HandleAsync((TCommand)request, cancellationToken);
}

internal sealed class QueryWrapper<TQuery, TResult> : HandlerWrapper<TResult> where TQuery : IQuery<TResult>
{
    public override Task<TResult> HandleAsync(object request, IServiceProvider services, CancellationToken cancellationToken) =>
        services.GetRequiredService<IQueryHandler<TQuery, TResult>>().HandleAsync((TQuery)request, cancellationToken);
}

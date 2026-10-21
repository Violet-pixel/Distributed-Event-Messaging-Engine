using Brado.EventStore.Outbox;
using Brado.Workers.Messaging;
using Brado.Workers.Outbox;
using Brado.Workers.Projections;
using Brado.Workers.Sagas;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Brado.Workers;

public static class WorkersServiceCollectionExtensions
{
    public static IServiceCollection AddBradoWorkers(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<OutboxOptions>(configuration.GetSection(OutboxOptions.Section));
        services.Configure<MessagingOptions>(configuration.GetSection(MessagingOptions.Section));
        services.Configure<ProjectionOptions>(configuration.GetSection(ProjectionOptions.Section));
        services.Configure<SagaOptions>(configuration.GetSection(SagaOptions.Section));

        services.AddSingleton<IRetryPolicy, ExponentialBackoffRetryPolicy>();
        services.AddSingleton<OutboxMetrics>();
        services.AddSingleton<SharedBrokerConnection>();
        services.AddSingleton<InProcessMessagePublisher>();
        services.AddSingleton<InProcessMessageSubscriber>();
        services.AddSingleton<TcpMessagePublisher>();
        services.AddSingleton<TcpMessageSubscriber>();

        services.AddSingleton<IMessagePublisher>(sp =>
            sp.GetRequiredService<IOptions<MessagingOptions>>().Value.Mode == BrokerMode.Tcp
                ? sp.GetRequiredService<TcpMessagePublisher>()
                : sp.GetRequiredService<InProcessMessagePublisher>());
        services.AddSingleton<IMessageSubscriber>(sp =>
            sp.GetRequiredService<IOptions<MessagingOptions>>().Value.Mode == BrokerMode.Tcp
                ? sp.GetRequiredService<TcpMessageSubscriber>()
                : sp.GetRequiredService<InProcessMessageSubscriber>());

        services.AddSingleton<IPaymentGateway, SimulatedPaymentGateway>();
        services.AddSingleton<IInventoryService, SimulatedInventoryService>();
        services.AddScoped<OrderFulfillmentSaga>();

        services.AddSingleton<OutboxDispatcherService>();
        services.AddHostedService(sp => sp.GetRequiredService<OutboxDispatcherService>());
        services.AddHostedService<ProjectionWorker>();
        services.AddHostedService<SagaWorker>();
        return services;
    }
}

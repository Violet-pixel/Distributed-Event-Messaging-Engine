using Brado.Broker.Handlers;
using Brado.Broker.Metrics;
using Brado.Broker.Server;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Brado.Broker;

public static class BrokerServiceCollectionExtensions
{
    public static IServiceCollection AddBradoBroker(this IServiceCollection services, IConfiguration configuration, bool hostTcpServer = true)
    {
        services.Configure<BrokerOptions>(configuration.GetSection(BrokerOptions.Section));

        services.TryAddSingleton<IMessageJournal, NullMessageJournal>();
        services.AddSingleton<BrokerMetrics>();
        services.AddSingleton<ITopicFactory>(sp => new DefaultTopicFactory(sp.GetRequiredService<IOptions<BrokerOptions>>().Value));
        services.AddSingleton<MessageBroker>();
        services.AddSingleton<IMessageBroker>(sp => sp.GetRequiredService<MessageBroker>());
        services.AddSingleton<ConnectionRegistry>();
        services.AddSingleton<BrokerMonitor>();

        services.AddSingleton<IFrameHandler, HelloHandler>();
        services.AddSingleton<IFrameHandler, PingHandler>();
        services.AddSingleton<IFrameHandler, PublishHandler>();
        services.AddSingleton<IFrameHandler, SubscribeHandler>();
        services.AddSingleton<IFrameHandler, UnsubscribeHandler>();
        services.AddSingleton<IFrameHandler, AckHandler>();
        services.AddSingleton<IFrameHandler, NackHandler>();
        services.AddSingleton<FrameHandlerRegistry>();

        if (hostTcpServer)
        {
            services.AddSingleton<BrokerServer>();
            services.AddHostedService(sp => sp.GetRequiredService<BrokerServer>());
        }

        return services;
    }
}

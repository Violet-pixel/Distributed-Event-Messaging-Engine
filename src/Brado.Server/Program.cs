using System.Text.Json.Serialization;
using Brado.Broker;
using Brado.Cqrs;
using Brado.EventStore;
using Brado.Server.Api;
using Brado.Workers;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddBradoBroker(builder.Configuration)
    .AddBradoEventStore(builder.Configuration)
    .AddBradoCqrs(builder.Configuration)
    .AddBradoWorkers(builder.Configuration);

builder.Services.Configure<SimulationOptions>(builder.Configuration.GetSection(SimulationOptions.Section));
builder.Services.AddSingleton<DashboardService>();
builder.Services.AddSingleton<TrafficSimulator>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<TrafficSimulator>());

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
    options.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
});

var app = builder.Build();

await app.Services.EnsureEventStoreSchemaAsync();
await app.Services.EnsureReadModelAsync();

app.UseBradoErrors();
app.UseDefaultFiles();
app.UseStaticFiles();
app.MapBradoApi();

app.Run();

public partial class Program
{
}

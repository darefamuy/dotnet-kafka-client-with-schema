using Confluent.SchemaRegistry;
using HCI.Kafka.Avro.Consumer;
using HCI.Kafka.Avro.SchemaRegistry;
using HCI.Kafka.Avro.SchemaRegistry.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serilog;
using Serilog.Events;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate:
        "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {SourceContext}: {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    Log.Information("Starting HCI Avro Consumer (Module 3)");

    var builder = Host.CreateApplicationBuilder(args);
    builder.Services.AddSerilog();

    builder.Services.Configure<KafkaOptions>(
        builder.Configuration.GetSection(KafkaOptions.SectionName));
    builder.Services.Configure<SchemaRegistryOptions>(
        builder.Configuration.GetSection(SchemaRegistryOptions.SectionName));

    builder.Services.AddSingleton<ISchemaRegistryClient>(sp =>
    {
        var opts = sp.GetRequiredService<IOptions<SchemaRegistryOptions>>();
        var logger = sp.GetRequiredService<ILogger<CachedSchemaRegistryClient>>();
        return SchemaRegistryClientFactory.Create(opts, logger);
    });

    // Three consumer BackgroundServices — each subscribes to its own topic
    builder.Services.AddHostedService<AvroMedicinalProductConsumer>();
    builder.Services.AddHostedService<AvroDrugAlertConsumer>();
    builder.Services.AddHostedService<AvroPriceUpdateConsumer>();

    var host = builder.Build();
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "HCI Avro Consumer terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}

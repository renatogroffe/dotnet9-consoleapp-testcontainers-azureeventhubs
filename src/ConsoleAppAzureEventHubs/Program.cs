using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using Bogus.DataSets;
using ConsoleAppAzureEventHubs.Utils;
using Serilog;
using System.Text;
using Testcontainers.EventHubs;

var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("testcontainers-azureeventhubs.tmp")
    .CreateLogger();
logger.Information("***** Iniciando testes com Testcontainers + Azure Event Hubs *****");

var configFile = Path.Combine(Directory.GetCurrentDirectory(), "Config.json");
logger.Information($"Arquivo de configuracoes do Azure Event Hubs: {configFile}");

CommandLineHelper.Execute("docker container ls",
    "Containers antes da execucao do Testcontainers...");

var eventHubTestsName = "eventhub-teste";
var consumerGroup01 = "consumer01";
var consumerGroup02 = "consumer02";
var eventHubsConfig = EventHubsServiceConfiguration.Create()
    .WithEntity(eventHubTestsName, partitionCount: 4,
        [consumerGroup01, consumerGroup02]);
var eventHubsContainer = new EventHubsBuilder()
    .WithImage("mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.1.0")
    .WithAcceptLicenseAgreement(true)
    .WithConfigurationBuilder(eventHubsConfig)
    .Build();
await eventHubsContainer.StartAsync();

CommandLineHelper.Execute("docker container ls",
    "Containers apos execucao do Testcontainers...");

var connectionAzureEventHubs = eventHubsContainer.GetConnectionString();
logger.Information($"Connection String = {connectionAzureEventHubs}");
logger.Information($"Event Hub (topico) a ser utilizado nos testes = {eventHubTestsName}");

await using var producer = new EventHubProducerClient(
    eventHubsContainer.GetConnectionString(), eventHubTestsName);
using var eventDataBatch = await producer.CreateBatchAsync();
const int maxMessages = 15;
var lorem = new Lorem("pt_BR");
for (int i = 1; i <= maxMessages; i++)
{
    var sentence = lorem.Sentence();
    logger.Information($"Adicionando ao EventDataBatch mensagem {i}/{maxMessages}: {sentence}");
    eventDataBatch.TryAdd(new EventData(sentence));
}
logger.Information("Enviar mensagens para o Event Hub...");
await producer.SendAsync(eventDataBatch);
logger.Information("Mensagens enviadas para o Event Hub com sucesso!");
logger.Information("Pressione ENTER para continuar...");
Console.ReadLine();

await ConsumeMessageUsingGroupAsync(consumerGroup01);
logger.Information("Pressione ENTER para continuar...");
Console.ReadLine();

await ConsumeMessageUsingGroupAsync(consumerGroup02);
logger.Information("Pressione ENTER para continuar...");
Console.ReadLine();

Console.WriteLine("Testes concluidos com sucesso!");


async Task ConsumeMessageUsingGroupAsync(string consumerGroup)
{
    logger.Information($"Processamento para o consumer group: {consumerGroup}");

    await using var consumer = new EventHubConsumerClient(
        consumerGroup,
        connectionAzureEventHubs,
        eventHubName: eventHubTestsName);

    var readOptions = new ReadEventOptions();
    readOptions.MaximumWaitTime = TimeSpan.FromSeconds(5);

    var asyncEnumerator = consumer.ReadEventsAsync(readOptions);
    await foreach (var partitionEvent in asyncEnumerator)
    {
        if (partitionEvent.Data is null)
            break;
        logger.Information($"Mensagem recebida: " +
            $"SequenceNumber = {partitionEvent.Data.SequenceNumber} | " +
            $"Body = {Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray())}");
    }

    logger.Information($"Encerrado o processamento para o Consumer Group: {consumerGroup}");
}

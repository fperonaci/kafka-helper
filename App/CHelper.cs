using Confluent.Kafka;

using Streamiz.Kafka.Net.SerDes;

namespace App;

public class Deserializer : IDeserializer<string>
{
    private readonly static StringSerDes StringSerDes = new();

    public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        return StringSerDes.Deserialize(data.ToArray(), context);
    }
}

public static class CHelper
{
    public static void Consume(string server, string topic, string groupId, AutoOffsetReset offsetReset)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = server,
            GroupId = groupId,
            AutoOffsetReset = offsetReset
        };

        var consumer = new ConsumerBuilder<string, string>(config)
            .SetKeyDeserializer(new Deserializer())
            .SetValueDeserializer(new Deserializer())
            .Build();

        consumer.Subscribe(topic);

        Console.WriteLine();
        Console.WriteLine($"Consuming messages with group id {groupId}..");

        Console.CancelKeyPress += delegate
        {
            Console.WriteLine();
            Console.WriteLine("I'm being shut down, good bye!");
        };

        while (true)
        {
            var result = consumer.Consume();
            var key = result.Message.Key;
            var value = result.Message.Value;
            var partition = result.Partition;
            var timestamp = result.Message.Timestamp;
            Console.WriteLine($"{partition} {key} {value} {timestamp.UtcDateTime.ToLocalTime():HH:mm:ss}");
        }
    }

    public static void Consume(string server, string topic, string groupId) =>
        Consume(server, topic, groupId, AutoOffsetReset.Earliest);
}

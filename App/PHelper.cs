using Confluent.Kafka;

using Streamiz.Kafka.Net.SerDes;

namespace App;

public class Serializer : ISerializer<string>
{
    private readonly static StringSerDes StringSerDes = new();

    public byte[] Serialize(string data, SerializationContext context)
    {
        return StringSerDes.Serialize(data, context);
    }
}

public static class PHelper
{
    public static void Produce(string server, string topic)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = server,
            Partitioner = Partitioner.Murmur2Random
        };

        using var producer = new ProducerBuilder<string, string>(config)
            .SetKeySerializer(new Serializer())
            .SetValueSerializer(new Serializer())
            .Build();


        Console.WriteLine();
        Console.WriteLine($"Producing messages on topic {topic}..");

        Console.CancelKeyPress += delegate
        {
            Console.WriteLine();
            Console.WriteLine("I'm being shut down, good bye!");
        };

        while (true)
        {
            Console.WriteLine("Enter key and value..");
            var input = Console.ReadLine()!.Split(' ');
            var message = new Message<string, string>
            {
                Key = input[0],
                Value = input[1]
            };
            if (!string.IsNullOrEmpty(message.Key))
            {
                producer.Produce(topic, message);
                producer.Flush();
            }
        }
    }
}

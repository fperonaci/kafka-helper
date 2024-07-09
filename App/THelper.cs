using Confluent.Kafka;

namespace App;

public static class THelper
{
    private static IAdminClient CreateAdminClient(string server)
    {
        var config = new AdminClientConfig()
        {
            BootstrapServers = server
        };

        return new AdminClientBuilder(config).Build();

    }
    public static IEnumerable<string> GetConsumerGroupsIds(string server)
    {
        using var client = CreateAdminClient(server);

        return client.ListConsumerGroupsAsync().Result.Valid.Select(x => x.GroupId);
    }

    public static IEnumerable<string> GetTopicsNames(string server)
    {
        using var client = CreateAdminClient(server);

        return client.GetMetadata(TimeSpan.FromSeconds(60)).Topics.Select(x => x.Topic);
    }

    public static IEnumerable<int> GetTopicsNumberOfPartitions(string server)
    {
        using var client = CreateAdminClient(server);

        return client.GetMetadata(TimeSpan.FromSeconds(60)).Topics.Select(x => x.Partitions.Count);
    }

    public static IEnumerable<(string, int)> GetTopicsNamesAndNumberOfPartitions(string server)
    {
        using var client = CreateAdminClient(server);

        var x = client.GetMetadata(TimeSpan.FromSeconds(60)).Topics.Select(x => x.Topic);
        var y = client.GetMetadata(TimeSpan.FromSeconds(60)).Topics.Select(x => x.Partitions.Count);

        return x.Zip(y);
    }

    public static void CreateTopicIfNotExists(string server, string name, int numPartitions)
    {
        if (GetTopicsNames(server).Contains(name))
        {
            return;
        }

        using var client = CreateAdminClient(server);

        client.CreateTopicsAsync([new() { Name = name, NumPartitions = numPartitions }]).Wait();
    }

    public static void DeleteTopicIfExists(string server, string name)
    {
        if (!GetTopicsNames(server).Contains(name))
        {
            return;
        }

        using var client = CreateAdminClient(server);

        client.DeleteTopicsAsync([name]).Wait();
    }
}

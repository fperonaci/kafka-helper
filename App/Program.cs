using App;

var server = args[0];

if (args.Length != 0)
{
    switch (args[1])
    {
        case "list-topics":
            Console.WriteLine($"Existing topics (<name>, <numPartitions>):");
            THelper.GetTopicsNamesAndNumberOfPartitions(server)
            .ToList().ForEach(x => Console.WriteLine(x));
            return;

        case "create-topic":
            var numPartitions = args.Length > 3 ? int.Parse(args[3]) : 1;
            THelper.CreateTopicIfNotExists(server, args[2], numPartitions);
            return;

        case "delete-topic":
            THelper.DeleteTopicIfExists(server, args[2]);
            return;

        case "delete-all-topics":
            foreach (var topic in THelper.GetTopicsNames(server))
                THelper.DeleteTopicIfExists(server, topic);
            return;

        case "produce":
            PHelper.Produce(server, args[2]);
            return;

        case "consume":
            var groupId = args.Length > 3 ? args[3] : Guid.NewGuid().ToString();
            CHelper.Consume(server, args[2], groupId);
            return;
    }
}

Console.WriteLine("Usage:");
Console.WriteLine("dotnet run --project App -- list-topics");
Console.WriteLine("dotnet run --project App -- create-topic <topic> [<numPartitions>]");
Console.WriteLine("dotnet run --project App -- delete-topic <topic>");
Console.WriteLine("dotnet run --project App -- delete-all-topics");
Console.WriteLine("dotnet run --project App -- produce <topic>");
Console.WriteLine("dotnet run --project App -- consume <topic> [<groupId>]");

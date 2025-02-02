﻿using App;

if (args.Length < 2)
{
    PrintHelp();
    return;
}

var server = args[0];

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

static void PrintHelp()
{
    Console.WriteLine("Usage:");
    Console.WriteLine("dotnet run --project App -- <host>:<port> list-topics");
    Console.WriteLine("dotnet run --project App -- <host>:<port> create-topic <topic> [<numPartitions>]");
    Console.WriteLine("dotnet run --project App -- <host>:<port> delete-topic <topic>");
    Console.WriteLine("dotnet run --project App -- <host>:<port> delete-all-topics");
    Console.WriteLine("dotnet run --project App -- <host>:<port> produce <topic>");
    Console.WriteLine("dotnet run --project App -- <host>:<port> consume <topic> [<groupId>]");
}

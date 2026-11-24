using Tedd.HostMesh;

var client = File.Exists("/mesh/key") ? new LocalMeshClient("/mesh/agent.sock", Convert.FromBase64String(File.ReadAllText("/mesh/key").Trim())) : null;
using var endpoint = client == null ? null : await client.BindDatagramAsync(4050);
using var listener = client == null ? null : await client.ListenAsync(4052);
var streamServer = listener == null ? Task.CompletedTask : ServeStreams();
Directory.CreateDirectory("/data/files");
var count = File.Exists("/data/files/starts") ? int.Parse(File.ReadAllText("/data/files/starts")) : 0;
File.WriteAllText("/data/files/starts", (count + 1).ToString());
File.Delete("/data/files/stall");
var heartbeat = Heartbeat();
File.WriteAllText("/tmp/ready", "");
if (endpoint == null) await Task.Delay(Timeout.InfiniteTimeSpan);
while (endpoint != null)
{
    var packet = await endpoint.ReceiveAsync();
    await endpoint.SendToAsync(packet.Source, packet.Payload);
}
await streamServer;
await heartbeat;

async Task Heartbeat()
{
    while (!File.Exists("/data/files/stall"))
    {
        File.WriteAllText("/tmp/alive", "1");
        await Task.Delay(500);
    }
}

async Task ServeStreams()
{
    while (true)
    {
        using var connection = await listener!.AcceptAsync();
        var request = new byte[1];
        await connection.Stream.ReadExactlyAsync(request);
        var response = System.Text.Encoding.UTF8.GetBytes(connection.Peer.EffectiveEndPoint.ToString());
        await connection.Stream.WriteAsync(response);
        connection.CompleteWrites();
    }
}

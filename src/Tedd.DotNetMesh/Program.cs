using System.Runtime.InteropServices;
using System.Text.Json;
using Tedd.DotNetMesh;
using Tedd.DotNetPod;
using Tedd.Tdfs;

if (args.Length == 0 || args.Length != (args[0] switch
{
    "worker" or "status" or "gateway" => 2,
    "apply" or "delete" => 3,
    "publish" => 4,
    _ => -1
}))
{
    Console.Error.WriteLine("dotnetmesh <worker|gateway|status|apply|delete|publish> cluster.json [deployment.json|name] [published-directory]");
    return 2;
}
using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; shutdown.Cancel(); };
using var signal = OperatingSystem.IsLinux() ? PosixSignalRegistration.Create(PosixSignal.SIGTERM, c => { c.Cancel = true; shutdown.Cancel(); }) : null;
var ct = shutdown.Token;
try
{
    var config = JsonSerializer.Deserialize<ClusterOptions>(await File.ReadAllTextAsync(args[1], ct)) ?? throw new InvalidDataException("Empty configuration.");
    if (config.ClusterId == Guid.Empty || config.Seeds.Any(s => !s.StartsWith("quic://", StringComparison.Ordinal)) || config.SpkiPins.Length == 0)
        throw new ArgumentException("Supply pinned QUIC seeds and an expected TDFS cluster ID.");
    await using var dfs = await DfsClient.ConnectAsync(new DfsClientOptions
    {
        ExpectedClusterId = config.ClusterId,
        SeedEndpoints = config.Seeds,
        Credentials = new(config.Principal, (await File.ReadAllTextAsync(config.SecretFile, ct)).TrimEnd('\r', '\n')),
        Security = new() { PinnedSpkiSha256 = config.SpkiPins.Select(Convert.FromBase64String).ToArray() },
        OperationTimeout = TimeSpan.FromSeconds(8),
        ClientName = "dotnetmesh"
    }, ct);
    var volume = dfs.GetVolume(config.Volume);
    var store = new ControlStore(volume);
    switch (args[0])
    {
        case "worker":
            var opts = config.Worker ?? throw new ArgumentException("Missing worker configuration.");
            using (var host = PodHost.Open(opts.Node, opts.EngineSocket))
            await using (var worker = new Worker(opts, volume, host)) await worker.RunAsync(ct);
            break;
        case "gateway":
            await new ServiceGateway(store, config.Gateway ?? throw new ArgumentException("Missing gateway configuration.")).RunAsync(ct);
            break;
        case "publish":
        case "apply":
            var d = JsonSerializer.Deserialize<Deployment>(await File.ReadAllTextAsync(args[2], ct)) ?? throw new InvalidDataException("Empty deployment.");
            d.Validate();
            if (args[0] == "publish") await ReleasePublisher.PublishAsync(volume, d, args[3], ct);
            else
            {
                await using var revision = await volume.Snapshots.OpenAsync(d.Snapshot, ct);
                if (!await revision.Files.ExistsAsync("/apps/" + d.Name + "/" + d.EntryPoint, ct)) throw new FileNotFoundException(d.EntryPoint);
                await store.ChangeAsync(s => s.Deployments[d.Name] = d, ct);
            }
            break;
        case "delete":
            PodSpec.ValidateId(args[2]);
            await store.ChangeAsync(s => s.Deployments.Remove(args[2]), ct);
            break;
        case "status": Console.WriteLine(JsonSerializer.Serialize(await store.ReadAsync(ct), new JsonSerializerOptions { WriteIndented = true })); break;
    }
    return 0;
}
catch (OperationCanceledException) when (ct.IsCancellationRequested) { return 0; }
catch (Exception error) { Console.Error.WriteLine(error.Message); return 1; }

public sealed record ClusterOptions(Guid ClusterId, string[] Seeds, string Volume, string Principal, string SecretFile, string[] SpkiPins, WorkerOptions? Worker, GatewayOptions? Gateway = null);

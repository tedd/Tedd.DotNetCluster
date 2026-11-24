using System.Net;
using System.Net.Sockets;
using Tedd.DotNetPod;
using Tedd.HostMesh;

namespace Tedd.DotNetMesh;

public sealed record ServiceRoute(int ListenPort, string Deployment, int TargetPort);
public sealed record GatewayOptions(MeshBinding Identity, ServiceRoute[] Routes);

public sealed class ServiceGateway
{
    readonly ControlStore store;
    readonly LocalMeshClient mesh;
    readonly ServiceRoute[] routes;
    volatile ClusterState latest = new();
    int rotation;

    public ServiceGateway(ControlStore store, GatewayOptions options)
    {
        this.store = store;
        mesh = WorkloadNetwork.Open(options.Identity.Socket, options.Identity.KeyFile);
        routes = options.Routes.ToArray();
        if (routes.Length is < 1 or > 64 || routes.Select(r => r.ListenPort).Distinct().Count() != routes.Length)
            throw new ArgumentException("Configure 1–64 distinct service ports.");
        foreach (var route in routes)
        {
            PodSpec.ValidateId(route.Deployment);
            if (route.ListenPort is < 1 or > 65535 || route.TargetPort is < 1 or > 65535) throw new ArgumentException("Invalid service port.");
        }
    }

    public static IPEndPoint[] Resolve(ClusterState state, ServiceRoute route, DateTimeOffset now)
    {
        if (!state.Deployments.TryGetValue(route.Deployment, out var desired)) return [];
        var endpoints = new List<IPEndPoint>();
        foreach (var (key, slot) in state.Slots)
            if (slot.Release.Name == desired.Name && slot.Ordinal < desired.Replicas && slot.Release.Mesh &&
                state.Nodes.TryGetValue(slot.Node, out var n) && !n.Drain && now - n.Seen < Scheduler.FailureTimeout &&
                n.Ready.GetValueOrDefault(key) == slot.Generation && IPAddress.TryParse(n.Addresses?.GetValueOrDefault(key), out var ip))
                endpoints.Add(new(ip, route.TargetPort));
        return endpoints.Distinct().OrderBy(ep => ep.Address.ToString(), StringComparer.Ordinal).ToArray();
    }

    public Task RunAsync(CancellationToken ct) => Task.WhenAll(routes.Select(r => Serve(r, ct)).Append(Refresh(ct)));

    async Task Refresh(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { latest = await store.ReadAsync(ct); }
            catch (Exception error) when (!ct.IsCancellationRequested) { Console.Error.WriteLine($"Service discovery: {error.Message}"); }
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }
    }

    async Task Serve(ServiceRoute route, CancellationToken ct)
    {
        var sessions = new HashSet<Task>();
        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    using var listener = await mesh.ListenAsync(route.ListenPort, ct);
                    while (!ct.IsCancellationRequested)
                    {
                        var flow = await listener.AcceptAsync(ct);
                        sessions.RemoveWhere(t => t.IsCompleted);
                        if (sessions.Count >= 256) { flow.Dispose(); continue; }
                        sessions.Add(Forward(flow, route, ct));
                    }
                }
                catch (Exception e) when (!ct.IsCancellationRequested && e is IOException or SocketException)
                { Console.Error.WriteLine($"Mesh listener {route.ListenPort}: {e.Message}"); }
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
        }
        finally { await Task.WhenAll(sessions); }
    }

    async Task Forward(LocalMeshConnection incoming, ServiceRoute route, CancellationToken ct)
    {
        using var source = incoming;
        var endpoints = Resolve(latest, route, DateTimeOffset.UtcNow);
        uint start = unchecked((uint)Interlocked.Increment(ref rotation));
        for (uint n = 0; n < endpoints.Length; n++)
        {
            LocalMeshConnection? target = null;
            try
            {
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(ct);
                attempt.CancelAfter(TimeSpan.FromSeconds(3));
                target = await mesh.ConnectFromAsync(endpoints[(start + n) % (uint)endpoints.Length], source.Peer.EffectiveEndPoint, attempt.Token);
            }
            catch (Exception e) when (e is IOException or SocketException or OperationCanceledException)
            { if (ct.IsCancellationRequested) return; continue; }
            using (target)
            using (var end = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                try { await Task.WhenAll(Pump(source, target), Pump(target, source)); }
                catch (Exception e) when (e is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
                async Task Pump(LocalMeshConnection a, LocalMeshConnection b)
                {
                    try { await a.Stream.CopyToAsync(b.Stream, 16384, end.Token); b.CompleteWrites(); }
                    catch { await end.CancelAsync(); throw; }
                }
            }
            return;
        }
    }
}

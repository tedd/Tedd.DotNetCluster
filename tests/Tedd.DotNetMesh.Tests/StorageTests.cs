using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.DotNetPod;
using Tedd.HostMesh;
using Tedd.Tdfs;
using Tedd.Tdfs.Testing;

namespace Tedd.DotNetMesh.Tests;

public class StorageTests
{
    static TestClusterOptions Options => new() { ScenarioTimeout = TimeSpan.FromMinutes(3) };

    [Fact]
    public Task ConcurrentControllersPreserveAllUpdatesThroughLeaderFailure() =>
        ClusterTest.RunAsync(Options, async cluster =>
        {
            var ct = TestContext.Current.CancellationToken;
            await using var first = await cluster.ConnectAsync(new() { Name = "first" }, ct);
            await using var second = await cluster.ConnectAsync(new() { Name = "second" }, ct);
            var stores = new[] { new ControlStore(first.GetVolume("vol")), new ControlStore(second.GetVolume("vol")) };
            await Task.WhenAll(Enumerable.Range(0, 8).Select(i => stores[i % 2].ChangeAsync(s =>
            {
                var d = SchedulerTests.App with { Name = "app-" + i };
                s.Deployments[d.Name] = d;
            }, ct)));
            var leader = await cluster.GetLeaderAsync("vol", ct);
            await leader.CrashAsync();
            try
            {
                await cluster.GetLeaderAsync("vol", ct);
                var state = await stores[1].ChangeAsync(s => s.Deployments.Remove("app-0"), ct);
                Assert.Equal(7, state.Deployments.Count);
                Assert.Equal(state.Deployments.Keys.Order(), (await stores[0].ReadAsync(ct)).Deployments.Keys.Order());
            }
            finally { await leader.RestartAsync(); await cluster.WaitForStableAsync(ct: ct); }
        }, TestContext.Current.CancellationToken);

    public static bool Live => OperatingSystem.IsLinux() && Environment.GetEnvironmentVariable("MESH_TEST_DIRECTORY") != null;

    [Fact(SkipUnless = nameof(Live), Skip = "Requires the Linux FUSE3 bridge.")]
    public Task MountedRandomWritesRenamesAndRevocationUseTheReplicatedVolume() =>
        ClusterTest.RunAsync(Options, async cluster =>
        {
            var ct = TestContext.Current.CancellationToken;
            var target = Directory.CreateDirectory(Environment.GetEnvironmentVariable("MESH_TEST_DIRECTORY") + "/fs-" + Guid.NewGuid().ToString("N")[..8]).FullName;
            await using var dfs = await cluster.ConnectAsync(ct: ct);
            var volume = dfs.GetVolume("vol");
            await volume.Directories.CreateDirectoryAsync("/data/slot", ct);
            var store = new ControlStore(volume);
            var placement = new Placement("a", 0, SchedulerTests.App, "first");
            await store.ChangeAsync(s => s.Slots["svc-0"] = placement, ct);
            using var mount = new VolumeMount(target, "/data/slot", volume.Files, volume.Directories, new ReplicaFence(volume, "svc-0", "first"));
            var path = Path.Combine(target, "hello");
            await File.WriteAllTextAsync(path, "abcdef", ct);
            using (var file = File.Open(path, FileMode.Open, FileAccess.Write))
            {
                file.Position = 2;
                file.WriteByte((byte)'Z');
                file.SetLength(4);
                file.Flush(true);
            }
            Assert.Equal("abZd", await volume.Files.ReadAllTextAsync("/data/slot/hello", ct: ct));
            var renamed = Path.Combine(target, "renamed");
            File.Move(path, renamed);
            Assert.Equal(new[] { "renamed" }, Directory.GetFiles(target).Select(Path.GetFileName));
            File.Delete(renamed);
            Assert.False(await volume.Files.ExistsAsync("/data/slot/renamed", ct));
            await store.ChangeAsync(s => s.Slots["svc-0"] = placement with { Generation = "next" }, ct);
            Assert.Throws<UnauthorizedAccessException>(() => File.WriteAllText(path, "stale"));
            Assert.False(await volume.Files.ExistsAsync("/data/slot/hello", ct));
        }, TestContext.Current.CancellationToken);

    [Fact(SkipUnless = nameof(Live), Skip = "Requires a Linux FUSE mount, a disposable container engine and the published mesh fixture.")]
    public Task ContainerExecutesSnapshotWritesReplicatedDataAndUsesItsMeshAddress() =>
        ClusterTest.RunAsync(Options, async cluster =>
        {
            var ct = TestContext.Current.CancellationToken;
            var root = Environment.GetEnvironmentVariable("MESH_TEST_DIRECTORY")! + "/run-" + Guid.NewGuid().ToString("N")[..8];
            Directory.CreateDirectory(root);
            var fixture = Environment.GetEnvironmentVariable("MESH_TEST_FIXTURE")!;
            await using var dfs = await cluster.ConnectAsync(ct: ct);
            await using var observer = await cluster.ConnectAsync(new() { Name = "observer" }, ct);
            var volume = dfs.GetVolume("vol");
            var read = observer.GetVolume("vol");
            await volume.Directories.CreateDirectoryAsync("/apps/echo", ct);
            foreach (var file in Directory.EnumerateFiles(fixture).Where(f => f.EndsWith(".dll") || f.EndsWith(".json")))
                await volume.Files.WriteAllBytesAsync("/apps/echo/" + Path.GetFileName(file), await File.ReadAllBytesAsync(file, ct), ct);
            await volume.Snapshots.CreateAsync("v1", ct: ct);
            // A running revision must read the snapshot even after the publisher updates the live tree.
            await volume.Files.WriteAllBytesAsync("/apps/echo/MeshFixture.dll", new byte[] { 1, 2, 3 }, ct);

            var appIp = IPAddress.Parse("10.88.1.11"); var clientIp = IPAddress.Parse("10.88.1.12");
            var gatewayIp = IPAddress.Parse("10.88.1.13");
            using var identityKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
            var request = new CertificateRequest("CN=mesh-test", identityKey, HashAlgorithmName.SHA256);
            using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(1));
            var members = new MeshMembership([new Member(1, cert, Ingress: true)], new Dictionary<IPAddress, ushort> { [appIp] = 1, [clientIp] = 1, [gatewayIp] = 1 });
            var allowed = System.Net.IPNetwork.Parse("10.88.1.0/24");
            var links = new QuiclyLinks(1, members, cert, null, new Dictionary<ushort, IPEndPoint>());
            await using var agent = await MeshAgent.StartAsync(1, members, cert, links, [
                new("pod",appIp,[new(allowed,4051)]),new("tester",clientIp,[new(allowed,4050),new(allowed,8040)],true),
                new("gateway",gatewayIp,[new(allowed,4052)],true)], cancellationToken: ct);
            var key = RandomNumberGenerator.GetBytes(32);
            var binding = new MeshBinding(root + "/mesh.sock", root + "/app.key", appIp.ToString());
            await File.WriteAllTextAsync(binding.KeyFile, Convert.ToBase64String(key), ct);
            await using var ipc = LocalMeshServer.Listen(agent.Register("pod"), binding.Socket, key, allowed);
            if (!OperatingSystem.IsLinux()) throw new PlatformNotSupportedException();
            File.SetUnixFileMode(binding.Socket, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupWrite | UnixFileMode.OtherWrite);
            using var caller = agent.Register("tester").BindDatagram(4051, allowed);

            var d = new Deployment("echo", "v1", Environment.GetEnvironmentVariable("POD_TEST_IMAGE")!,
                "MeshFixture.dll", Mesh: true, ReadinessFile: "ready", LivenessFile: "alive", LivenessTimeoutSeconds: 6);
            var options = new WorkerOptions("test-" + Guid.NewGuid().ToString("N")[..8], "zone", root + "/worker", 512, 2000,
                new() { ["echo-0"] = binding }, Environment.GetEnvironmentVariable("POD_TEST_SOCKET")!);
            using var host = PodHost.Open(options.Node, options.EngineSocket);
            var store = new ControlStore(volume);
            await store.ChangeAsync(s => s.Deployments[d.Name] = d, ct);
            await using (var worker = new Worker(options, volume, host))
            {
                await worker.RecoverAsync(ct);
                await AwaitReady(worker, host, ct);
                Assert.Equal("1", await read.Files.ReadAllTextAsync("/data/echo/0/files/starts", ct: ct));
                Assert.True(caller.SendTo(new(appIp, 4050), new byte[] { 3, 1, 4 }));
                using var replyTimeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                replyTimeout.CancelAfter(TimeSpan.FromSeconds(10));
                var reply = await caller.ReceiveAsync(replyTimeout.Token);
                Assert.Equal(appIp, reply.Source.Address);
                Assert.Equal(new byte[] { 3, 1, 4 }, reply.Payload);
                await worker.TickAsync(ct);
                var edgeKey = RandomNumberGenerator.GetBytes(32);
                var edge = new MeshBinding(root + "/edge.sock", root + "/edge.key");
                await File.WriteAllTextAsync(edge.KeyFile, Convert.ToBase64String(edgeKey), ct);
                await using var edgeIpc = LocalMeshServer.Listen(agent.Register("gateway"), edge.Socket, edgeKey, allowed);
                using var stopGateway = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var serving = new ServiceGateway(store, new(edge, [new(8040, "echo", 4052)])).RunAsync(stopGateway.Token);
                try
                {
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    timeout.CancelAfter(TimeSpan.FromSeconds(15));
                    var source = new IPEndPoint(IPAddress.Parse("192.0.2.31"), 48732);
                    var tester = agent.Register("tester");
                    MeshConnection? inbound = null;
                    while (inbound == null)
                    {
                        try { inbound = await tester.ConnectFromAsync(new(gatewayIp, 8040), source, timeout.Token); }
                        catch (IOException) { await Task.Delay(100, timeout.Token); }
                    }
                    await using var trusted = inbound;
                    await trusted.Stream.WriteAsync(new byte[] { 1 }, timeout.Token);
                    await trusted.CompleteWritesAsync(timeout.Token);
                    using var response = new StreamReader(trusted.Stream);
                    Assert.Equal(source.ToString(), await response.ReadToEndAsync(timeout.Token));
                    await Assert.ThrowsAnyAsync<IOException>(() => WorkloadNetwork.Open(binding.Socket, binding.KeyFile)
                        .ConnectFromAsync(new(clientIp, 4051), source, timeout.Token));
                }
                finally
                {
                    await stopGateway.CancelAsync();
                    try { await serving; } catch (OperationCanceledException) when (stopGateway.IsCancellationRequested) { }
                }
                var healthySince = System.Diagnostics.Stopwatch.StartNew();
                while (healthySince.Elapsed < TimeSpan.FromSeconds(8))
                {
                    await worker.TickAsync(ct);
                    Assert.Equal("1", await read.Files.ReadAllTextAsync("/data/echo/0/files/starts", ct: ct));
                    Assert.True((await host.InspectAsync("echo-0", ct))!.Ready);
                    await Task.Delay(500, ct);
                }
                await volume.Files.WriteAllTextAsync("/data/echo/0/files/stall", "stop heartbeat", ct: ct);
                using var repair = CancellationTokenSource.CreateLinkedTokenSource(ct);
                repair.CancelAfter(TimeSpan.FromSeconds(30));
                while (await read.Files.ReadAllTextAsync("/data/echo/0/files/starts", ct: repair.Token) != "2")
                {
                    await worker.TickAsync(repair.Token);
                    await Task.Delay(300, repair.Token);
                }
            }
            await using (var restarted = new Worker(options, volume, host))
            {
                await restarted.RecoverAsync(ct);
                await AwaitReady(restarted, host, ct);
                Assert.Equal("3", await read.Files.ReadAllTextAsync("/data/echo/0/files/starts", ct: ct));
                await store.ChangeAsync(s => s.Deployments.Clear(), ct);
                await restarted.TickAsync(ct);
                Assert.Empty(await host.ListAsync(ct));
            }
        }, TestContext.Current.CancellationToken);

    static async Task AwaitReady(Worker worker, PodHost host, CancellationToken ct)
    {
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(TimeSpan.FromSeconds(45));
        do
        {
            await worker.TickAsync(limit.Token);
            if ((await host.InspectAsync("echo-0", limit.Token))?.Ready == true) return;
            await Task.Delay(350, limit.Token);
        } while (true);
    }
}

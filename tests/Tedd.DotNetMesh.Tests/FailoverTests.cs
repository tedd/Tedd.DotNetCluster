using System.Diagnostics;
using System.Net;
using System.Text.Json;
using Tedd.DotNetPod;
using Tedd.Tdfs;
using Tedd.Tdfs.Testing;

namespace Tedd.DotNetMesh.Tests;

public class FailoverTests
{
    [Fact]
    public Task OrderedHandoffPreservesFinalWritesAndRevokesTheOldOwnerAtAcknowledgement() =>
        ClusterTest.RunAsync(TestClusterOptions.SingleNode, async cluster =>
        {
            var ct = TestContext.Current.CancellationToken;
            await using var client = await cluster.ConnectAsync(ct: ct);
            var volume = client.GetVolume("vol");
            var store = new ControlStore(volume);
            var app = SchedulerTests.App with { Replicas = 1 };
            var next = app with { Image = "sha256:" + new string('b', 64), Snapshot = "next" };
            var key = app.Slot(0);
            var now = DateTimeOffset.UtcNow;
            var state = await store.ChangeAsync(s =>
            {
                s.Nodes["a"] = new("a", "east", now, 512, 2000, false, [], new(), [app.Image], SupportsStopHandoff: true);
                s.Nodes["b"] = new("b", "west", now, 512, 2000, false, [], new(), [next.Image], SupportsStopHandoff: true);
                s.Deployments[app.Name] = app;
                Scheduler.Reconcile(s, now);
            }, ct);
            var old = state.Slots[key];
            state = await store.ChangeAsync(s =>
            {
                s.Deployments[app.Name] = next;
                Scheduler.Reconcile(s, now);
            }, ct);
            Assert.True(state.Slots[key].Stopping);
            var outgoing = new ReplicaFence(volume, key, old.Generation);
            outgoing.Mutate((files, _) => files.WriteAllText("/shutdown-checkpoint", "complete"));
            state = await store.ChangeAsync(s => Scheduler.ConfirmStopped(s, key, old.Node, old.Generation, now), ct);
            Assert.Equal("b", state.Slots[key].Node);
            Assert.Throws<UnauthorizedAccessException>(() => outgoing.Mutate((files, _) => files.WriteAllText("/shutdown-checkpoint", "stale")));
            Assert.Equal("complete", new ReplicaFence(volume, key, state.Slots[key].Generation).Mutate((files, _) => files.ReadAllText("/shutdown-checkpoint")));
        }, TestContext.Current.CancellationToken);

    [Fact]
    public Task RelocationRejectsAnInFlightMutationFromTheOldOwner() =>
        ClusterTest.RunAsync(TestClusterOptions.SingleNode, async cluster =>
        {
            var ct = TestContext.Current.CancellationToken;
            await using var first = await cluster.ConnectAsync(ct: ct);
            await using var second = await cluster.ConnectAsync(new() { Name = "other" }, ct);
            var volume = first.GetVolume("vol");
            var store = new ControlStore(second.GetVolume("vol"));
            var a = new Placement("a", 0, SchedulerTests.App, "old");
            await store.ChangeAsync(s => s.Slots["service-0"] = a, ct);
            var stale = new ReplicaFence(volume, "service-0", "old");
            var readFence = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var write = Task.Run(() => stale.Mutate((files, _) =>
            {
                readFence.TrySetResult();
                resume.Task.WaitAsync(TimeSpan.FromSeconds(15), ct).GetAwaiter().GetResult();
                files.WriteAllText("/old-write", "old");
                return 0;
            }), ct);
            try
            {
                await readFence.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
                await store.ChangeAsync(s => s.Slots["service-0"] = a with { Node = "b", Generation = "new" }, ct);
            }
            finally { resume.TrySetResult(); }
            await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await write);
            Assert.False(await volume.Files.ExistsAsync("/old-write", ct));
            new ReplicaFence(volume, "service-0", "new").Mutate((files, _) => files.WriteAllText("/new-write", "new"));
            Assert.Equal("new", await volume.Files.ReadAllTextAsync("/new-write", ct: ct));
        }, TestContext.Current.CancellationToken);

    public static bool Live => StorageTests.ContainerAvailable;

    [Fact(SkipUnless = nameof(Live), Skip = "Requires two worker processes, loopback QUIC, FUSE and a disposable Linux engine.")]
    public Task KilledWorkerIsReplacedAndRejoiningWorkerReleasesItsOrphan() =>
        ClusterTest.RunAsync(new() { Transport = TransportKind.Quic, ScenarioTimeout = TimeSpan.FromMinutes(4) }, async cluster =>
        {
            var ct = TestContext.Current.CancellationToken;
            var root = Environment.GetEnvironmentVariable("MESH_TEST_DIRECTORY")! + "/ha-" + Guid.NewGuid().ToString("N")[..6];
            Directory.CreateDirectory(root);
            var connection = cluster.CreateClientOptions();
            Assert.All(connection.SeedEndpoints, s => Assert.True(IPAddress.IsLoopback(IPAddress.Parse(new Uri(s).Host))));
            var secret = root + "/secret";
            await File.WriteAllTextAsync(secret, connection.Credentials.Secret, ct);
            await using var dfs = await cluster.ConnectAsync(ct: ct);
            var volume = dfs.GetVolume("vol"); var store = new ControlStore(volume);
            var app = new Deployment("echo", "release", Environment.GetEnvironmentVariable("POD_TEST_IMAGE")!, "MeshFixture.dll", ReadinessFile: "ready");
            var control = new ClusterOptions(cluster.ClusterId, connection.SeedEndpoints.ToArray(), "vol", connection.Credentials.Principal,
                secret, connection.Security.PinnedSpkiSha256.Select(b => Convert.ToBase64String(b)).ToArray(), null);
            var manifest = Path.Combine(root, "release.json");
            var config = Path.Combine(root, "cluster.json");
            await File.WriteAllTextAsync(manifest, JsonSerializer.Serialize(app), ct);
            await File.WriteAllTextAsync(config, JsonSerializer.Serialize(control), ct);
            await Command("publish", manifest, Environment.GetEnvironmentVariable("MESH_TEST_FIXTURE")!);
            await Command("apply", manifest);
            var observed = JsonSerializer.Deserialize<ClusterState>(await Command("status"));
            Assert.Equal(app, observed!.Deployments["echo"]);
            var engine = Environment.GetEnvironmentVariable("POD_TEST_SOCKET")!;
            var prefix = "ha-" + Guid.NewGuid().ToString("N")[..6];
            string nodeA = prefix + "-a", nodeB = prefix + "-b";
            using var hostA = PodHost.Open(nodeA, engine);
            using var hostB = PodHost.Open(nodeB, engine);
            async Task<Process> Start(string name)
            {
                var path = root + "/" + name + ".json";
                var opts = control with { Worker = new(name, name, root + "/" + name, 512, 2000, new(), engine) };
                await File.WriteAllTextAsync(path, JsonSerializer.Serialize(opts), ct);
                var start = new ProcessStartInfo("dotnet") { UseShellExecute = false };
                start.ArgumentList.Add(typeof(Worker).Assembly.Location);
                start.ArgumentList.Add("worker"); start.ArgumentList.Add(path);
                return Process.Start(start) ?? throw new IOException("Worker failed to start.");
            }
            async Task<string> Command(string verb, params string[] arguments)
            {
                var start = new ProcessStartInfo("dotnet") { UseShellExecute = false, RedirectStandardOutput = true,
                    ArgumentList = { typeof(Worker).Assembly.Location, verb, config } };
                foreach (var arg in arguments) start.ArgumentList.Add(arg);
                using var command = Process.Start(start)!;
                var output = await command.StandardOutput.ReadToEndAsync(ct);
                await command.WaitForExitAsync(ct);
                Assert.Equal(0, command.ExitCode);
                return output;
            }
            async Task Eventually(Func<Task<bool>> predicate)
            {
                using var bound = CancellationTokenSource.CreateLinkedTokenSource(ct);
                bound.CancelAfter(TimeSpan.FromSeconds(75));
                while (!await predicate()) await Task.Delay(250, bound.Token);
            }
            Process? first = null, second = null;
            try
            {
                first = await Start(nodeA);
                await Eventually(async () => (await hostA.InspectAsync("echo-0", ct))?.Ready == true);
                second = await Start(nodeB);
                await Eventually(async () => (await store.ReadAsync(ct)).Nodes.ContainsKey(nodeB));
                first.Kill(); await first.WaitForExitAsync(ct); first.Dispose(); first = null;
                await Eventually(async () => (await hostB.InspectAsync("echo-0", ct))?.Ready == true);
                Assert.Equal("2", await volume.Files.ReadAllTextAsync("/data/echo/0/files/starts", ct: ct));
                first = await Start(nodeA);
                await Eventually(async () => await hostA.InspectAsync("echo-0", ct) == null && (await store.ReadAsync(ct)).Nodes.ContainsKey(nodeA));
                Assert.True((await hostB.InspectAsync("echo-0", ct))!.Ready);
                await Command("delete", "echo");
                await Eventually(async () => await hostB.InspectAsync("echo-0", ct) == null);
            }
            finally
            {
                foreach (var process in new[] { first, second })
                {
                    if (process == null) continue;
                    if (!process.HasExited)
                    {
                        using var signal = Process.Start(new ProcessStartInfo("kill") { ArgumentList = { "-TERM", process.Id.ToString() }, UseShellExecute = false });
                        await signal!.WaitForExitAsync(ct);
                        try { await process.WaitForExitAsync(ct).WaitAsync(TimeSpan.FromSeconds(18), ct); }
                        catch (TimeoutException) { process.Kill(); await process.WaitForExitAsync(ct); }
                    }
                    process.Dispose();
                }
                await hostA.UnloadAsync("echo-0", 0, ct);
                await hostB.UnloadAsync("echo-0", 0, ct);
            }
        }, TestContext.Current.CancellationToken);
}

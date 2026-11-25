using Tedd.SMB3;
using Tedd.Tdfs.Testing;

namespace Tedd.DotNetMesh.Tests;

public class WorkerSmbTests
{
    public static bool LinuxMountAvailable => StorageTests.Live;

    [Fact(SkipUnless = nameof(LinuxMountAvailable), Skip = "Requires the Linux FUSE3 bridge.")]
    public Task HostWritesThroughTheGenerationFence() =>
        ClusterTest.RunAsync(new() { ScenarioTimeout = TimeSpan.FromMinutes(3) }, async cluster =>
        {
            var ct = TestContext.Current.CancellationToken;
            var root = Directory.CreateDirectory(Path.Combine(Environment.GetEnvironmentVariable("MESH_TEST_DIRECTORY")!,
                "smb-" + Guid.NewGuid().ToString("N")));
            try
            {
                var target = root.CreateSubdirectory("data").FullName;
                var passwordFile = Path.Combine(root.FullName, "secret");
                await File.WriteAllTextAsync(passwordFile, "test-pass", ct);
                await using var dfs = await cluster.ConnectAsync(ct: ct);
                var volume = dfs.GetVolume("vol");
                const string subtree = "/data/svc/0";
                await volume.Directories.CreateDirectoryAsync(subtree, ct);
                var store = new ControlStore(volume);
                var placement = new Placement("worker", 0, SchedulerTests.App, "first");
                await store.ChangeAsync(s => s.Slots["svc-0"] = placement, ct);
                using var mount = new VolumeMount(target, subtree, volume.Files, volume.Directories, new(volume, "svc-0", "first"));
                await using var host = WorkerSmbHost.Start(new("127.0.0.1", 0, new() { ["writer"] = passwordFile },
                    new() { ["svc-0"] = new([], ["writer"]) }));
                await host.AddAsync("svc-0", target, ct);
                await using var client = await SmbClient.ConnectAsync("127.0.0.1", "svc-0",
                    new() { Port = host.LocalEndPoint.Port, UserName = "writer", Password = "test-pass" }, ct);
                byte[] payload = [4, 5];
                await client.WriteAllBytesAsync("file", payload, ct);
                Assert.Equal(payload, await volume.Files.ReadAllBytesAsync(subtree + "/file", ct));
                await store.ChangeAsync(s => s.Slots["svc-0"] = placement with { Generation = "next" }, ct);
                await Assert.ThrowsAsync<SmbException>(() => client.WriteAllBytesAsync("stale", payload, ct));
                Assert.False(await volume.Files.ExistsAsync(subtree + "/stale", ct));
            }
            finally { root.Delete(true); }
        }, TestContext.Current.CancellationToken);

    [Fact]
    public async Task HostRevokesOldTreesAndHandlesWhileOtherPodSharesRemainAvailable()
    {
        var ct = TestContext.Current.CancellationToken;
        var root = Directory.CreateTempSubdirectory("mesh-smb-");
        try
        {
            var readerFile = Path.Combine(root.FullName, "reader-secret");
            var writerFile = Path.Combine(root.FullName, "writer-secret");
            await File.WriteAllTextAsync(readerFile, "read-pass\n", ct);
            await File.WriteAllTextAsync(writerFile, "write-pass\n", ct);
            var files = Directory.CreateDirectory(Path.Combine(root.FullName, "files")).FullName;
            var logs = Directory.CreateDirectory(Path.Combine(root.FullName, "logs")).FullName;
            await using var host = WorkerSmbHost.Start(new("127.0.0.1", 0,
                new() { ["reader"] = readerFile, ["writer"] = writerFile },
                new() { ["*"] = new([], ["writer"]), ["files-0"] = new(["reader"], ["writer"]) }));
            await host.AddAsync("files-0", files, ct);
            await host.AddAsync("files-0", files, ct);
            await host.AddAsync("logs-0", logs, ct);
            Task<SmbClient> Connect(string slot, string user, string password) =>
                SmbClient.ConnectAsync("127.0.0.1", slot, new() { Port = host.LocalEndPoint.Port, UserName = user, Password = password }, ct);

            await Assert.ThrowsAsync<SmbException>(() => Connect("files-0", "reader", "wrong-pass"));
            await Assert.ThrowsAsync<SmbException>(() => Connect("logs-0", "reader", "read-pass"));
            await using var writer = await Connect("files-0", "writer", "write-pass");
            await using var reader = await Connect("files-0", "READER", "read-pass");
            await using var other = await Connect("logs-0", "writer", "write-pass");
            Assert.True(reader.IsEncrypted);
            byte[] payload = [1, 2, 3];
            await writer.WriteAllBytesAsync("payload", payload, ct);
            Assert.Equal(payload, await reader.ReadAllBytesAsync("payload", ct));
            await Assert.ThrowsAsync<SmbException>(() => reader.WriteAllBytesAsync("denied", payload, ct));
            Assert.False(File.Exists(Path.Combine(files, "denied")));

            var oldHandle = await reader.OpenAsync("payload", ct: ct);
            await host.RemoveAsync("files-0", ct);
            await host.RemoveAsync("files-0", ct);
            await Assert.ThrowsAsync<SmbException>(() => Connect("files-0", "writer", "write-pass"));
            await host.AddAsync("files-0", files, ct);
            await Assert.ThrowsAsync<SmbException>(() => oldHandle.ReadAsync(new byte[1], 0, ct));
            await Assert.ThrowsAsync<SmbException>(() => oldHandle.DisposeAsync().AsTask());
            await using var replacement = await Connect("files-0", "reader", "read-pass");
            Assert.Equal(payload, await replacement.ReadAllBytesAsync("payload", ct));
            await other.WriteAllBytesAsync("still-open", payload, ct);
            Assert.Equal(payload, await File.ReadAllBytesAsync(Path.Combine(logs, "still-open"), ct));
        }
        finally { root.Delete(true); }
    }
}

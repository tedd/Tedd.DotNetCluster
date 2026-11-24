using Tedd.Tdfs.Testing;

namespace Tedd.DotNetMesh.Tests;

public class PublicationTests
{
    [Fact]
    public Task PublishingReplacesTheLiveTreeAndKeepsBothSnapshots() =>
        ClusterTest.RunAsync(TestClusterOptions.SingleNode, async cluster =>
        {
            var ct = TestContext.Current.CancellationToken;
            await using var client = await cluster.ConnectAsync(ct: ct);
            var volume = client.GetVolume("vol");
            var source = Directory.CreateTempSubdirectory("mesh-release-");
            try
            {
                Directory.CreateDirectory(Path.Combine(source.FullName, "assets"));
                var dll = Path.Combine(source.FullName, "Service.dll");
                var asset = Path.Combine(source.FullName, "assets", "name");
                await File.WriteAllBytesAsync(dll, new byte[] { 1, 2, 3 }, ct);
                await File.WriteAllTextAsync(asset, "before", ct);
                var deployment = SchedulerTests.App;
                await ReleasePublisher.PublishAsync(volume, deployment, source.FullName, ct);
                await File.WriteAllBytesAsync(dll, new byte[] { 4, 5, 6 }, ct);
                File.Delete(asset);
                await ReleasePublisher.PublishAsync(volume, deployment with { Snapshot = "r2" }, source.FullName, ct);
                await using var old = await volume.Snapshots.OpenAsync("r1", ct);
                await using var current = await volume.Snapshots.OpenAsync("r2", ct);
                Assert.Equal("before", await old.Files.ReadAllTextAsync("/apps/service/assets/name", ct: ct));
                Assert.False(await current.Files.ExistsAsync("/apps/service/assets/name", ct));
                Assert.Equal(new byte[] { 1, 2, 3 }, await old.Files.ReadAllBytesAsync("/apps/service/Service.dll", ct));
                Assert.Equal(new byte[] { 4, 5, 6 }, await current.Files.ReadAllBytesAsync("/apps/service/Service.dll", ct));
                await File.WriteAllBytesAsync(dll, new byte[] { 7 }, ct);
                await Assert.ThrowsAsync<Tedd.Tdfs.DfsAlreadyExistsException>(() =>
                    ReleasePublisher.PublishAsync(volume, deployment, source.FullName, ct));
                Assert.Equal(new byte[] { 4, 5, 6 }, await volume.Files.ReadAllBytesAsync("/apps/service/Service.dll", ct));
                File.Delete(dll);
                await Assert.ThrowsAsync<FileNotFoundException>(() => ReleasePublisher.PublishAsync(volume, deployment with { Snapshot = "r3" }, source.FullName, ct));
                Assert.Equal(2, (await volume.Snapshots.ListAsync(ct)).Count);
            }
            finally { source.Delete(true); }
        }, TestContext.Current.CancellationToken);
}

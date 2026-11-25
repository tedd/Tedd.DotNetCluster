using System.IO.Compression;
using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography;
using Tedd.Tdfs.Testing;

namespace Tedd.DotNetMesh.Tests;

public class GitHubReleaseTests
{
    [Fact]
    public Task LatestZipReplacesTheDesiredSnapshotAndRetriesKeepItsIdentity() =>
        ClusterTest.RunAsync(TestClusterOptions.SingleNode, async cluster =>
        {
            var ct = TestContext.Current.CancellationToken;
            await using var client = await cluster.ConnectAsync(ct: ct);
            var volume = client.GetVolume("vol");
            var app = SchedulerTests.App with { PersistentDirectories = [new("logs", "/app/logs")] };
            var firstZip = Zip((app.EntryPoint, "v1"), ("assets/old.txt", "old"));
            var nextZip = Zip((app.EntryPoint, "v2"), ("assets/current.txt", "current"), ("empty/", ""));
            using var first = new HttpClient(new GitHub(firstZip));
            using var next = new HttpClient(new GitHub(nextZip, multiple: true));
            var old = await GitHubReleases.DeployLatestAsync(first, volume, app, "test/project", null, "test-token", ct);
            await Assert.ThrowsAsync<InvalidDataException>(() => GitHubReleases.DeployLatestAsync(next, volume, app,
                "test/project", null, "test-token", ct));
            var updated = await GitHubReleases.DeployLatestAsync(next, volume, app, "test/project", "linux.zip", "test-token", ct);
            var retry = await GitHubReleases.DeployLatestAsync(next, volume, app, "test/project", "linux.zip", "test-token", ct);
            Assert.Equal(updated.Snapshot, retry.Snapshot);
            Assert.NotEqual(old.Snapshot, updated.Snapshot);
            Assert.Equal(updated.Snapshot, (await new ControlStore(volume).ReadAsync(ct)).Deployments[app.Name].Snapshot);
            Assert.Equal(2, (await volume.Snapshots.ListAsync(ct)).Count);
            await using var before = await volume.Snapshots.OpenAsync(old.Snapshot, ct);
            await using var after = await volume.Snapshots.OpenAsync(updated.Snapshot, ct);
            Assert.Equal("v1", await before.Files.ReadAllTextAsync("/apps/service/Service.dll", ct: ct));
            Assert.Equal("v2", await after.Files.ReadAllTextAsync("/apps/service/Service.dll", ct: ct));
            Assert.False(await after.Files.ExistsAsync("/apps/service/assets/old.txt", ct));
            Assert.True(await after.Directories.ExistsAsync("/apps/service/empty", ct));
            Assert.True(await after.Directories.ExistsAsync("/apps/service/logs", ct));
            var remapped = app with { PersistentDirectories = [new("logs", "/app/var/logs")] };
            var mapped = await GitHubReleases.DeployLatestAsync(next, volume, remapped, "test/project", "linux.zip", "test-token", ct);
            await using var remappedView = await volume.Snapshots.OpenAsync(mapped.Snapshot, ct);
            Assert.True(await remappedView.Directories.ExistsAsync("/apps/service/var/logs", ct));
        }, TestContext.Current.CancellationToken);

    [Fact]
    public Task InvalidArtifactsPreserveThePriorSnapshotAndDeployment() =>
        ClusterTest.RunAsync(TestClusterOptions.SingleNode, async cluster =>
        {
            var ct = TestContext.Current.CancellationToken;
            await using var client = await cluster.ConnectAsync(ct: ct);
            var volume = client.GetVolume("vol");
            var app = SchedulerTests.App with { PersistentDirectories = [new("logs", "/app/logs")] };
            var valid = Zip((app.EntryPoint, "working"));
            using var first = new HttpClient(new GitHub(valid));
            var original = await GitHubReleases.DeployLatestAsync(first, volume, app, "test/project", null, "test-token", ct);
            async Task Deploy(byte[] bytes, string? digest = null)
            {
                using var http = new HttpClient(new GitHub(bytes, digest: digest));
                await GitHubReleases.DeployLatestAsync(http, volume, app, "test/project", null, "test-token", ct);
            }
            await Assert.ThrowsAsync<InvalidDataException>(() => Deploy(valid, "sha256:" + new string('0', 64)));
            await Assert.ThrowsAsync<FileNotFoundException>(() => Deploy(Zip(("other.dll", ""))));
            await Assert.ThrowsAsync<ArgumentException>(() => Deploy(Zip((app.EntryPoint, ""), ("../escape", ""))));
            await Assert.ThrowsAsync<InvalidDataException>(() => Deploy(Zip((app.EntryPoint, "1"), (app.EntryPoint, "2"))));
            await Assert.ThrowsAnyAsync<IOException>(() => Deploy(Zip((app.EntryPoint, "updated"), ("logs", "file"))));
            await Assert.ThrowsAsync<InvalidDataException>(() => Deploy([1, 2, 3]));
            Assert.Equal(original.Snapshot, (await new ControlStore(volume).ReadAsync(ct)).Deployments[app.Name].Snapshot);
            Assert.Single(await volume.Snapshots.ListAsync(ct));
            Assert.Equal("working", await volume.Files.ReadAllTextAsync("/apps/service/Service.dll", ct: ct));
        }, TestContext.Current.CancellationToken);

    static byte[] Zip(params (string Path, string Contents)[] entries)
    {
        using var buffer = new MemoryStream();
        using (var zip = new ZipArchive(buffer, ZipArchiveMode.Create, true))
            foreach (var (path, contents) in entries)
            {
                using var stream = zip.CreateEntry(path).Open();
                stream.Write(System.Text.Encoding.UTF8.GetBytes(contents));
            }
        return buffer.ToArray();
    }

    sealed class GitHub(byte[] zip, bool multiple = false, string? digest = null) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var api = "https://api.github.com/repos/test/project/releases/";
            Assert.Equal("Bearer test-token", request.Headers.Authorization?.ToString());
            Assert.NotEmpty(request.Headers.UserAgent);
            if (request.RequestUri!.AbsoluteUri == api + "latest")
            {
                var asset = new { id = 17, name = "linux.zip", size = zip.Length, state = "uploaded",
                    digest = digest ?? "sha256:" + Convert.ToHexStringLower(SHA256.HashData(zip)) };
                var assets = multiple ? new[] { asset, asset with { id = 18, name = "other.zip" } } : [asset];
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(new { assets }) });
            }
            Assert.Equal(api + "assets/17", request.RequestUri.AbsoluteUri);
            Assert.Equal("application/octet-stream", request.Headers.Accept.Single().MediaType);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(zip) });
        }
    }
}

using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Tedd.Tdfs;

namespace Tedd.DotNetMesh;

public static class GitHubReleases
{
    const long MaxDownloadBytes = 256L * 1024 * 1024;
    sealed record Asset(long Id, string Name, string State, long Size, string? Digest);
    sealed record Release(Asset[] Assets);

    public static async Task<Deployment> DeployLatestAsync(HttpClient http, DfsVolume volume, Deployment template,
        string repository, string? assetName, string? token, CancellationToken ct)
    {
        if (!Regex.IsMatch(repository, @"\A[A-Za-z0-9-]+/[A-Za-z0-9_.-]+\z") || repository.Split('/')[1] is "." or "..")
            throw new ArgumentException("Specify the GitHub repository as OWNER/REPO.");
        var api = $"https://api.github.com/repos/{repository}/releases/";
        var store = new ControlStore(volume);
        (template with { Snapshot = "github" }).Validate();
        var previous = (await store.ReadAsync(ct)).Deployments.GetValueOrDefault(template.Name);
        using var metadata = await GetAsync("latest", "application/vnd.github+json");
        await metadata.Content.LoadIntoBufferAsync(4 * 1024 * 1024, ct);
        await using var json = await metadata.Content.ReadAsStreamAsync(ct);
        var latest = await JsonSerializer.DeserializeAsync<Release>(json, JsonSerializerOptions.Web, ct);
        var candidates = latest?.Assets?.Where(a => a.State == "uploaded" && a.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase)
            && (assetName == null || assetName == a.Name)).ToArray();
        if (candidates is not [var asset])
            throw new InvalidDataException("Select one uploaded ZIP asset from the latest GitHub release. Specify its name when multiple ZIPs are available.");
        if (asset.Id <= 0 || asset.Size is <= 0 or > MaxDownloadBytes)
            throw new InvalidDataException("Release ZIP must be between 1 byte and 256 MiB.");

        using var download = await GetAsync($"assets/{asset.Id}", "application/octet-stream");
        await download.Content.LoadIntoBufferAsync(asset.Size, ct);
        await using var archive = await download.Content.ReadAsStreamAsync(ct);
        if (archive.Length != asset.Size) throw new InvalidDataException("Release ZIP length differs from GitHub metadata.");
        var digest = Convert.ToHexStringLower(await SHA256.HashDataAsync(archive, ct));
        if (asset.Digest != null && !string.Equals(asset.Digest, "sha256:" + digest, StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Release ZIP digest differs from GitHub metadata.");
        var mountPoints = string.Join('\n', template.ApplicationMountPoints().Order(StringComparer.Ordinal));
        var identity = SHA256.HashData(Encoding.UTF8.GetBytes(template.Name + "/" + digest + "\n" + mountPoints));
        var deployment = template with { Snapshot = "gh-" + Convert.ToHexStringLower(identity)[..44] };
        if (!await HasSnapshotAsync())
        {
            archive.Position = 0;
            try { await ReleasePublisher.PublishZipAsync(volume, deployment, archive, ct); }
            catch (DfsException ex) when (ex is DfsAlreadyExistsException or DfsTransactionConflictException)
            {
                if (!await HasSnapshotAsync()) throw;
            }
        }
        await store.ChangeAsync(state =>
        {
            var current = state.Deployments.GetValueOrDefault(deployment.Name);
            if (current?.Matches(deployment) == true) return;
            if (current == null ? previous != null : previous == null || !current.Matches(previous))
                throw new InvalidOperationException("Deployment changed during the download. Retry against its current revision.");
            state.Deployments[deployment.Name] = deployment;
        }, ct);
        return deployment;

        async Task<HttpResponseMessage> GetAsync(string endpoint, string accept)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, api + endpoint);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(accept));
            request.Headers.UserAgent.ParseAdd("Tedd.DotNetMesh/1.0");
            if (!string.IsNullOrWhiteSpace(token)) request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
            try { return response.EnsureSuccessStatusCode(); }
            catch { response.Dispose(); throw; }
        }

        async Task<bool> HasSnapshotAsync()
        {
            try
            {
                await using var existing = await volume.Snapshots.OpenAsync(deployment.Snapshot, ct);
                await deployment.ValidateApplicationAsync(existing.Files, existing.Directories, ct);
                return true;
            }
            catch (DfsException ex) when (ex.ErrorCode == DfsErrorCode.SnapshotNotFound) { return false; }
        }
    }
}

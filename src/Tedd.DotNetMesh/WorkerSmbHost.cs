using System.Net;
using Tedd.DotNetPod;
using Tedd.SMB3;

namespace Tedd.DotNetMesh;

public sealed record PodShareAcl(string[] Readers, string[] Writers);

public sealed record WorkerSmbOptions(string ListenAddress, int Port, Dictionary<string, string> PasswordFiles,
    Dictionary<string, PodShareAcl> Shares);

/// <summary>Exports the current generation's mounted ordinal data on the worker's SMB listener.</summary>
public sealed class WorkerSmbHost : IAsyncDisposable
{
    private sealed record Access(HashSet<string> Readers, HashSet<string> Writers);
    private readonly Dictionary<string, Access> policy = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> mounted = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim changes = new(1, 1);
    private readonly CancellationTokenSource stopping = new();
    private readonly SmbServer server;
    public IPEndPoint LocalEndPoint => server.LocalEndPoint;
    public Task Completion { get; }

    public static WorkerSmbHost Start(WorkerSmbOptions options) => new(options);

    private WorkerSmbHost(WorkerSmbOptions options)
    {
        if (options.Port is < 0 or > 65535 || !IPAddress.TryParse(options.ListenAddress, out var address))
            throw new ArgumentException("Specify an SMB listen IP address and TCP port.");
        ArgumentNullException.ThrowIfNull(options.PasswordFiles);
        ArgumentNullException.ThrowIfNull(options.Shares);
        var passwords = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, path) in options.PasswordFiles)
        {
            if (!Path.IsPathFullyQualified(path)) throw new ArgumentException("SMB password files require absolute host paths.");
            passwords.Add(name, File.ReadAllText(path).TrimEnd('\r', '\n'));
        }
        foreach (var (slot, acl) in options.Shares)
        {
            if (slot != "*") PodSpec.ValidateId(slot);
            if (acl?.Readers == null || acl.Writers == null) throw new ArgumentException($"Invalid SMB ACL for {slot}.");
            var readers = new HashSet<string>(acl.Readers, StringComparer.OrdinalIgnoreCase);
            var writers = new HashSet<string>(acl.Writers, StringComparer.OrdinalIgnoreCase);
            readers.UnionWith(writers);
            if (readers.Any(n => n == null || !passwords.ContainsKey(n)))
                throw new ArgumentException($"Unknown account in SMB ACL for {slot}.");
            policy.Add(slot, new(readers, writers));
        }
        server = new(new SmbServerOptions
        {
            ListenAddress = address, Port = options.Port, AllowEmptyShares = true,
            AuthenticationProvider = new NtlmAuthentication(passwords), Log = Console.Error.WriteLine
        });
        Completion = server.RunAsync(stopping.Token);
        if (Completion.IsCompleted)
        {
            try { Completion.GetAwaiter().GetResult(); }
            finally { server.DisposeAsync().AsTask().GetAwaiter().GetResult(); stopping.Dispose(); }
        }
    }

    public async Task AddAsync(string slot, string dataMount, CancellationToken ct = default)
    {
        PodSpec.ValidateId(slot);
        if (!policy.TryGetValue(slot, out var access) && !policy.TryGetValue("*", out access)) return;
        if (access.Readers.Count == 0) return;
        await changes.WaitAsync(ct);
        try
        {
            if (mounted.TryGetValue(slot, out var current))
            {
                if (current == dataMount) return;
                await server.RemoveShareAsync(slot, ct);
                mounted.Remove(slot);
            }
            await server.AddShareAsync(new(slot, new PodShareDirectory(dataMount))
            {
                Writable = access.Writers.Count > 0,
                Authorize = access.Readers.Contains, AuthorizeWrite = access.Writers.Contains
            }, ct);
            mounted.Add(slot, dataMount);
        }
        finally { changes.Release(); }
    }

    public async Task RemoveAsync(string slot, CancellationToken ct = default)
    {
        await changes.WaitAsync(ct);
        try
        {
            if (!mounted.ContainsKey(slot)) return;
            await server.RemoveShareAsync(slot, ct);
            mounted.Remove(slot);
        }
        finally { changes.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        await stopping.CancelAsync();
        try { await Completion; }
        finally { await server.DisposeAsync(); stopping.Dispose(); }
    }

    // The FUSE mount owns timestamps and exposes fixed DOS attributes.
    private sealed class PodShareDirectory(string root) : SmbDirectoryProvider(root)
    {
        public override SmbFileMetadata? GetMetadata(string path)
        {
            var entry = base.GetMetadata(path);
            return entry == null ? null : entry with { Attributes = entry.IsDirectory ? FileAttributes.Directory : FileAttributes.Archive };
        }

        public override void SetMetadata(string path, SmbMetadataUpdate update)
        {
            if (update.CreationTimeUtc != null || update.ChangeTimeUtc != null ||
                update.LastWriteTimeUtc != null || update.LastAccessTimeUtc != null)
                throw new NotSupportedException("The pod volume assigns file timestamps.");
            if (update.Attributes is { } attributes && attributes != GetMetadata(path)?.Attributes)
                throw new NotSupportedException("The pod volume assigns file attributes.");
        }

        public override void MarkChanged(string path, bool archive) { }
    }
}

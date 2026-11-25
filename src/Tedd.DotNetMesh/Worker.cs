using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using Tedd.DotNetPod;
using Tedd.HostMesh;
using Tedd.Tdfs;

namespace Tedd.DotNetMesh;

public sealed record WorkerOptions(string Node, string Zone, string RuntimeDirectory, int MemoryMiB, int MilliCpu,
    Dictionary<string, MeshBinding> Bindings, string EngineSocket = "/var/run/docker.sock", WorkerSmbOptions? Smb = null);

public static class WorkloadNetwork
{
    public static LocalMeshClient Open(string socket = "/mesh/agent.sock", string keyFile = "/mesh/key") =>
        new(socket, Convert.FromBase64String(File.ReadAllText(keyFile).Trim()));
}

public sealed class Worker : IAsyncDisposable
{
    sealed record Active(string Generation, string Root, DfsSnapshotView Snapshot, List<VolumeMount> Mounts, PodSpec Spec,
        AllocationSocket? Network, bool ShareRegistered = false);
    readonly WorkerOptions config;
    readonly PodHost host;
    readonly DfsVolume volume;
    readonly ControlStore store;
    readonly FileStream owner;
    readonly WorkerSmbHost? smb;
    readonly ConcurrentDictionary<string, Active> active = new();
    readonly ConcurrentDictionary<string, string> ready = new();
    readonly Dictionary<string, (int Attempts, long Retry)> failures = new();
    readonly ConcurrentDictionary<string, long> failedSlots = new();
    readonly TimeProvider clock;
    long lastContact;

    public Worker(WorkerOptions options, DfsVolume volume, PodHost host, TimeProvider? clock = null)
    {
        PodSpec.ValidateId(options.Node); PodSpec.ValidateId(options.Zone);
        if (options.MemoryMiB < 64 || options.MilliCpu < 10 || !Path.IsPathFullyQualified(options.RuntimeDirectory))
            throw new ArgumentException("Invalid worker resources or runtime directory.");
        config = options; this.host = host; this.volume = volume; this.clock = clock ?? TimeProvider.System;
        store = new(volume);
        Directory.CreateDirectory(config.RuntimeDirectory);
        owner = new(Path.Combine(config.RuntimeDirectory, "worker.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        try { if (options.Smb != null) smb = WorkerSmbHost.Start(options.Smb); }
        catch { owner.Dispose(); throw; }
        lastContact = this.clock.GetTimestamp();
    }

    public async Task RecoverAsync(CancellationToken ct)
    {
        // Bind mounts from a dead FUSE server must be replaced before its workloads can serve requests.
        foreach (var id in await host.ListAsync(ct)) await host.UnloadAsync(id, ct: ct);
        foreach (var entry in Directory.EnumerateDirectories(config.RuntimeDirectory, "alloc-*"))
        {
            if (!Guid.TryParseExact(Path.GetFileName(entry)[6..], "N", out _)) continue;
            DeleteAllocationDirectory(entry, recover: true);
        }
    }

    static void DeleteAllocationDirectory(string root, bool recover = false)
    {
        File.Delete(Path.Combine(root, "ipc"));
        foreach (var link in Directory.EnumerateFileSystemEntries(root, "data-*")) File.Delete(link);
        foreach (var leaf in new[] { "app", "data" })
        {
            var path = Path.Combine(root, leaf);
            if (recover && File.ReadLines("/proc/self/mountinfo").Any(l => l.Split(' ')[4] == EscapeMount(path) &&
                l.Contains(" - fuse.dotnetmesh ", StringComparison.Ordinal)) && umount2(path, 2) != 0)
                throw new IOException($"Could not detach {path}.");
            if (Directory.Exists(path)) Directory.Delete(path);
        }
        Directory.Delete(root);
    }

    static string EscapeMount(string path) => path.Replace("\\", "\\134").Replace(" ", "\\040").Replace("\t", "\\011").Replace("\n", "\\012");
    [DllImport("libc", SetLastError = true)] static extern int umount2(string path, int flags);

    public async Task RunAsync(CancellationToken ct)
    {
        await RecoverAsync(ct);
        var plans = Channel.CreateBounded<ClusterState>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = true
        });
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var heartbeats = ReportLoop();
        try
        {
            await foreach (var state in plans.Reader.ReadAllAsync(ct))
            {
                if (smb?.Completion.IsCompleted == true)
                {
                    await smb.Completion;
                    throw new IOException("The worker SMB listener stopped.");
                }
                try { await ApplyAsync(state, ct); }
                catch (Exception e) when (!ct.IsCancellationRequested)
                {
                    Console.Error.WriteLine($"Local reconciliation: {e.Message}");
                }
            }
        }
        finally
        {
            await lifetime.CancelAsync();
            try { await heartbeats; } catch (OperationCanceledException) when (lifetime.IsCancellationRequested) { }
            try { await DrainAsync(); }
            catch (Exception error) { Console.Error.WriteLine($"Worker drain: {error.Message}"); }
        }

        async Task ReportLoop()
        {
            try
            {
                while (!lifetime.IsCancellationRequested)
                {
                    try { plans.Writer.TryWrite(await HeartbeatAsync(false, lifetime.Token)); }
                    catch (Exception e) when (!lifetime.IsCancellationRequested)
                    {
                        Console.Error.WriteLine($"Worker heartbeat: {e.Message}");
                        if (clock.GetElapsedTime(lastContact) >= Scheduler.FailureTimeout) plans.Writer.TryWrite(new());
                    }
                    await Task.Delay(TimeSpan.FromSeconds(2), clock, lifetime.Token);
                }
            }
            finally { plans.Writer.TryComplete(); }
        }
    }

    async Task DrainAsync()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var state = await HeartbeatAsync(true, deadline.Token);
        using var completed = new CancellationTokenSource();
        var keepAlive = RenewWhileStopping();
        try { await ApplyAsync(state, CancellationToken.None); }
        finally
        {
            await completed.CancelAsync();
            try { await keepAlive; } catch (OperationCanceledException) when (completed.IsCancellationRequested) { }
        }

        async Task RenewWhileStopping()
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(2), clock, completed.Token);
                try { await HeartbeatAsync(true, completed.Token); }
                catch (Exception e) when (!completed.IsCancellationRequested) { Console.Error.WriteLine($"Drain heartbeat: {e.Message}"); }
            }
        }
    }

    async Task<ClusterState> HeartbeatAsync(bool drain, CancellationToken ct)
    {
        var keys = new List<string>();
        string[] images = [];
        if (!drain)
        {
            using var probe = CancellationTokenSource.CreateLinkedTokenSource(ct);
            probe.CancelAfter(TimeSpan.FromSeconds(3));
            try { images = await host.InstalledImagesAsync(probe.Token); }
            catch (Exception e) when (!ct.IsCancellationRequested && e is HttpRequestException or OperationCanceledException)
            { Console.Error.WriteLine($"Container engine unavailable: {e.Message}"); }
        }
        foreach (var (slot, binding) in config.Bindings)
        {
            try
            {
                _ = WorkloadNetwork.Open(binding.Socket, binding.KeyFile);
                using var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                await socket.ConnectAsync(new UnixDomainSocketEndPoint(binding.Socket), ct);
                keys.Add(slot);
            }
            catch (Exception e) when (e is IOException or SocketException or FormatException or ArgumentException or UnauthorizedAccessException) { }
        }
        var state = await store.ChangeAsync(s =>
        {
            var now = clock.GetUtcNow();
            var addresses = keys.Where(id => config.Bindings[id].Address != null).ToDictionary(id => id, id => config.Bindings[id].Address!);
            foreach (var pair in failedSlots)
                if (pair.Value <= clock.GetTimestamp()) failedSlots.TryRemove(pair);
            s.Nodes[config.Node] = new(config.Node, config.Zone, now, config.MemoryMiB, config.MilliCpu, drain, keys.ToArray(), new(ready), images, addresses, failedSlots.Keys.ToArray(), SupportsStopHandoff: true);
            Scheduler.Reconcile(s, now);
        }, ct);
        lastContact = clock.GetTimestamp();
        foreach (var (id, local) in active)
            if (state.Slots.TryGetValue(id, out var slot) && slot.Node == config.Node && slot.Generation == local.Generation)
                local.Network?.Renew();
            else local.Network?.Revoke();
        return state;
    }

    public async Task TickAsync(CancellationToken ct)
    {
        await ApplyAsync(await HeartbeatAsync(false, ct), ct);
    }

    async Task ApplyAsync(ClusterState state, CancellationToken ct)
    {
        var targets = state.Slots.Where(p => p.Value.Node == config.Node).ToDictionary();
        foreach (var (id, local) in active.ToArray())
            if (!targets.TryGetValue(id, out var target) || !target.Stopping && (target.Generation != local.Generation ||
                local.Spec.MeshSocket != null && local.Network?.Expired != false))
                await RemoveAsync(id, ct);

        foreach (var generation in failures.Keys.Except(targets.Values.Select(v => v.Generation)).ToArray()) failures.Remove(generation);
        foreach (var (id, target) in targets)
        {
            if (target.Stopping)
            {
                try
                {
                    if (active.ContainsKey(id)) await RemoveAsync(id, ct, 30);
                    else await host.UnloadAsync(id, 30, ct);
                    await store.ChangeAsync(s => Scheduler.ConfirmStopped(s, id, config.Node, target.Generation, clock.GetUtcNow()), ct);
                }
                catch (Exception error) when (!ct.IsCancellationRequested) { Console.Error.WriteLine($"Stopping {id}: {error.Message}"); }
                continue;
            }
            var retry = failures.GetValueOrDefault(target.Generation);
            if (clock.GetTimestamp() < retry.Retry) continue;
            try
            {
                if (!active.TryGetValue(id, out var local))
                {
                    local = await PrepareAsync(target, ct);
                    active[id] = local;
                }
                var status = await host.EnsureAsync(local.Spec, ct);
                if (smb != null && !local.ShareRegistered && status.State == "running")
                {
                    await smb.AddAsync(id, local.Spec.Data, ct);
                    active[id] = local with { ShareRegistered = true };
                }
                if (target.Release.LivenessFile != null && status.State == "running")
                {
                    var limit = clock.GetUtcNow().AddSeconds(-target.Release.LivenessTimeoutSeconds);
                    if (status.StartedAt < limit && (status.LastHeartbeat ?? status.StartedAt) < limit)
                    {
                        ready.TryRemove(id, out _);
                        await host.StopAsync(id, ct: ct);
                        throw new IOException("Liveness heartbeat expired. Restart scheduled.");
                    }
                }
                if (status.Ready) ready[id] = target.Generation;
                else ready.TryRemove(id, out _);
                failures.Remove(target.Generation);
            }
            catch (Exception error) when (!ct.IsCancellationRequested)
            {
                ready.TryRemove(id, out _);
                var seconds = Math.Min(60, 1 << Math.Min(6, retry.Attempts));
                failures[target.Generation] = (Math.Min(6, retry.Attempts + 1), clock.GetTimestamp() + seconds * clock.TimestampFrequency);
                if (retry.Attempts >= 2) failedSlots[id] = clock.GetTimestamp() + 60 * clock.TimestampFrequency;
                Console.Error.WriteLine($"{id}: {error.Message}");
            }
        }
    }

    async Task<Active> PrepareAsync(Placement slot, CancellationToken ct)
    {
        var d = slot.Release;
        d.Validate();
        if (!Guid.TryParseExact(slot.Generation, "N", out _) || slot.Ordinal is < 0 or >= 128)
            throw new InvalidDataException("Invalid allocation identity in control state.");
        var id = d.Slot(slot.Ordinal);
        var root = Path.Combine(config.RuntimeDirectory, "alloc-" + slot.Generation);
        var app = Path.Combine(root, "app"); var data = Path.Combine(root, "data");
        Directory.CreateDirectory(root);
        DfsSnapshotView? snapshot = null;
        List<VolumeMount> mounts = [];
        AllocationSocket? network = null;
        try
        {
            snapshot = await volume.Snapshots.OpenAsync(d.Snapshot, ct);
            var appRoot = "/apps/" + d.Name;
            await d.ValidateApplicationAsync(snapshot.Files, snapshot.Directories, ct);
            var dataRoot = d.DataRoot(slot.Ordinal);
            var mappings = d.PersistentDirectories ?? [];
            var fence = new ReplicaFence(volume, id, slot.Generation);
            fence.Mutate((_, directories) =>
            {
                directories.CreateDirectory(dataRoot);
                foreach (var mapping in mappings) directories.CreateDirectory(VolumeMount.ScopedPath(dataRoot, "/" + mapping.Source));
                return 0;
            });
            MeshBinding? binding = null;
            if (d.Mesh)
            {
                var upstream = config.Bindings[id];
                binding = upstream with { Socket = Path.Combine(root, "ipc") };
                network = new(binding.Socket, upstream.Socket, clock);
            }
            var spec = d.Template(slot.Ordinal, app, data, binding);
            spec.Validate();
            Directory.CreateDirectory(app);
            Directory.CreateDirectory(data);
            mounts.Add(new(app, appRoot, snapshot.Files, snapshot.Directories));
            mounts.Add(new(data, dataRoot, volume.Files, volume.Directories, fence));
            // Daemon-local links live outside the pod. All aliases bind the same FUSE inode and page cache.
            for (int i = 0; i < mappings.Length; i++)
                Directory.CreateSymbolicLink(spec.DirectoryMounts![i].Source, Path.Combine(data, mappings[i].Source));
            return new(slot.Generation, root, snapshot, mounts, spec, network);
        }
        catch
        {
            if (network != null) await network.DisposeAsync();
            foreach (var mount in mounts) mount.Dispose();
            if (snapshot != null) await snapshot.DisposeAsync();
            DeleteAllocationDirectory(root);
            throw;
        }
    }

    async Task RemoveAsync(string id, CancellationToken ct, int graceSeconds = 10)
    {
        ready.TryRemove(id, out _);
        var local = active[id];
        if (smb != null) await smb.RemoveAsync(id, ct);
        await host.UnloadAsync(id, graceSeconds, ct);
        if (local.Network != null) await local.Network.DisposeAsync();
        active[id] = local with { Network = null };
        foreach (var mount in local.Mounts) mount.Dispose();
        await local.Snapshot.DisposeAsync();
        DeleteAllocationDirectory(local.Root);
        active.TryRemove(id, out _);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            foreach (var id in active.Keys.ToArray()) await RemoveAsync(id, CancellationToken.None);
        }
        finally
        {
            try { if (smb != null) await smb.DisposeAsync(); }
            finally { owner.Dispose(); }
        }
    }
}

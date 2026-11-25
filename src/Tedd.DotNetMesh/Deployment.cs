using Tedd.DotNetPod;
using Tedd.Tdfs;

namespace Tedd.DotNetMesh;

public sealed record Deployment(string Name, string Snapshot, string Image, string EntryPoint, int Replicas = 1,
    int MemoryMiB = 256, int MilliCpu = 1000, int Pids = 128, bool Mesh = false, string? ReadinessFile = null,
    string? LivenessFile = null, int LivenessTimeoutSeconds = 30, DirectoryMapping[]? PersistentDirectories = null)
{
    public void Validate()
    {
        PodSpec.ValidateId(Name);
        if (Name.Length > 32 || Replicas is < 0 or > 128) throw new ArgumentException("Invalid deployment size.");
        PodSpec.ValidateId(Snapshot);
        if (LivenessFile != null) PodSpec.ValidateMarker(LivenessFile);
        if (LivenessTimeoutSeconds is < 5 or > 3600) throw new ArgumentOutOfRangeException(nameof(LivenessTimeoutSeconds));
        foreach (var mapping in PersistentDirectories ?? [])
        {
            if (string.IsNullOrEmpty(mapping?.Source)) throw new ArgumentException("A persistent directory needs a relative source.");
            if (!DfsPath.IsNormalized(VolumeMount.ScopedPath(DataRoot(127), "/" + mapping.Source)))
                throw new ArgumentException("Persistent directory sources must be canonical TDFS paths.");
        }
        Template(0, "/app/source", "/data/source", null).Validate();
        if (ApplicationMountPoints().Any(path => !DfsPath.IsNormalized(path)))
            throw new ArgumentException("Application mount points must be canonical TDFS paths.");
    }

    public string Slot(int ordinal) => $"{Name}-{ordinal}";
    internal string DataRoot(int ordinal) => $"/data/{Name}/{ordinal}";

    internal IEnumerable<string> ApplicationMountPoints() =>
        (PersistentDirectories ?? []).Where(m => m.Target.StartsWith("/app/", StringComparison.Ordinal))
            .Select(m => VolumeMount.ScopedPath("/apps/" + Name, m.Target[4..]));

    internal async Task ValidateApplicationAsync(DfsFileClient files, DfsDirectoryClient directories, CancellationToken ct)
    {
        if (!await files.ExistsAsync("/apps/" + Name + "/" + EntryPoint, ct)) throw new FileNotFoundException(EntryPoint);
        foreach (var path in ApplicationMountPoints())
            if (!await directories.ExistsAsync(path, ct)) throw new DirectoryNotFoundException($"Missing application mount point {path}.");
    }

    public bool Matches(Deployment other) => (this with { PersistentDirectories = other.PersistentDirectories }) == other &&
        (PersistentDirectories ?? []).OrderBy(m => m.Target, StringComparer.Ordinal).SequenceEqual(
            (other.PersistentDirectories ?? []).OrderBy(m => m.Target, StringComparer.Ordinal));

    public PodSpec Template(int ordinal, string app, string data, MeshBinding? binding) => new(Slot(ordinal),
        Image, app, EntryPoint, data, MemoryMiB, MilliCpu, Pids, binding?.Socket, binding?.KeyFile, ReadinessFile: ReadinessFile, LivenessFile: LivenessFile,
        DirectoryMounts: PersistentDirectories?.Select((m, i) => new DirectoryMapping(data + "-" + i, m.Target)).ToArray());
}

public sealed record MeshBinding(string Socket, string KeyFile, string? Address = null);
public sealed record WorkerReport(string Node, string Zone, DateTimeOffset Seen, int MemoryMiB, int MilliCpu,
    bool Drain, string[] Bindings, Dictionary<string, string> Ready, string[]? Images = null, Dictionary<string, string>? Addresses = null,
    string[]? FailedSlots = null, bool SupportsStopHandoff = false);
public sealed record Placement(string Node, int Ordinal, Deployment Release, string Generation, bool Stopping = false);

public sealed class ClusterState
{
    public Dictionary<string, Deployment> Deployments { get; init; } = new();
    public Dictionary<string, WorkerReport> Nodes { get; init; } = new();
    public Dictionary<string, Placement> Slots { get; init; } = new();
}

public static class Scheduler
{
    public static readonly TimeSpan FailureTimeout = TimeSpan.FromSeconds(30);

    public static void Reconcile(ClusterState state, DateTimeOffset now)
    {
        foreach (var node in state.Nodes.Values.Where(n => now - n.Seen >= FailureTimeout).ToArray())
            state.Nodes.Remove(node.Node);
        var nodes = state.Nodes.Values.Where(n => !n.Drain).ToDictionary(n => n.Node);
        bool handoff = state.Nodes.Values.All(n => n.SupportsStopHandoff);
        foreach (var (key, slot) in state.Slots.ToArray())
        {
            if (!state.Deployments.TryGetValue(slot.Release.Name, out var d) || slot.Ordinal >= d.Replicas ||
                !state.Nodes.TryGetValue(slot.Node, out var n))
                state.Slots.Remove(key);
            else if (n.Drain && handoff)
                state.Slots[key] = slot with { Stopping = true };
            else if (n.Drain || !Eligible(n, slot.Release, key))
                state.Slots.Remove(key);
            else if (slot.Stopping && !handoff)
                state.Slots[key] = slot with { Stopping = false };
        }

        foreach (var node in nodes.Values)
        {
            long memory = 0, cpu = 0;
            foreach (var (key, slot) in state.Slots.Where(p => p.Value.Node == node.Node).OrderBy(p => p.Key, StringComparer.Ordinal).ToArray())
            {
                if (memory + slot.Release.MemoryMiB > node.MemoryMiB || cpu + slot.Release.MilliCpu > node.MilliCpu)
                    state.Slots.Remove(key);
                else { memory += slot.Release.MemoryMiB; cpu += slot.Release.MilliCpu; }
            }
        }

        foreach (var d in state.Deployments.Values.OrderBy(x => x.Name, StringComparer.Ordinal))
        {
            var existing = state.Slots.Values.Where(s => s.Release.Name == d.Name).ToArray();
            if (existing.Any(s => s.Stopping)) continue;
            // Change one running replica at a time and wait for its new generation to become ready.
            var mayUpdate = existing.All(s => nodes[s.Node].Ready.GetValueOrDefault(d.Slot(s.Ordinal)) == s.Generation);
            // An operator can replace a broken rollout with a different revision.
            var recovering = existing.Where(s => nodes[s.Node].Ready.GetValueOrDefault(d.Slot(s.Ordinal)) != s.Generation)
                .OrderBy(s => s.Ordinal).FirstOrDefault();
            for (int i = 0; i < d.Replicas; i++)
            {
                var key = d.Slot(i);
                state.Slots.TryGetValue(key, out var current);
                if (current != null && ((current.Release with { Replicas = d.Replicas }).Matches(d) ||
                    !mayUpdate && current != recovering)) continue;
                var selected = SelectNode(state, nodes.Values, d, key, current?.Node);
                if (selected == null) continue;
                state.Slots[key] = current != null && handoff ? current with { Stopping = true } :
                    new(selected.Node, i, d, Guid.NewGuid().ToString("N"));
                if (current != null) mayUpdate = false;
            }
            if (mayUpdate && existing.Length == d.Replicas)
                RebalanceOne(d, state, nodes, handoff);
        }
    }

    /// <summary>Transfers ownership only after the outgoing worker confirms the matching container has stopped.</summary>
    public static void ConfirmStopped(ClusterState state, string key, string node, string generation, DateTimeOffset now)
    {
        if (!state.Slots.TryGetValue(key, out var old) || !old.Stopping || old.Node != node || old.Generation != generation) return;
        state.Slots.Remove(key);
        if (!state.Deployments.TryGetValue(old.Release.Name, out var desired) || old.Ordinal >= desired.Replicas) return;
        var candidates = state.Nodes.Values.Where(n => !n.Drain && now - n.Seen < FailureTimeout);
        var selected = SelectNode(state, candidates, desired, key, node);
        if (selected != null) state.Slots[key] = new(selected.Node, old.Ordinal, desired, Guid.NewGuid().ToString("N"));
    }

    static WorkerReport? SelectNode(ClusterState state, IEnumerable<WorkerReport> candidates, Deployment d, string key, string? currentNode)
    {
        var others = state.Slots.Where(p => p.Key != key).Select(p => p.Value).ToArray();
        var peers = others.Where(p => p.Release.Name == d.Name).ToArray();
        return candidates.Where(n => Eligible(n, d, key) && Fits(n, d, others))
            .OrderBy(n => peers.Count(p => state.Nodes.GetValueOrDefault(p.Node)?.Zone == n.Zone))
            .ThenBy(n => peers.Count(p => p.Node == n.Node)).ThenBy(n => n.Node == currentNode ? 0 : 1)
            .ThenBy(n => n.Node, StringComparer.Ordinal).FirstOrDefault();
    }

    static void RebalanceOne(Deployment deployment, ClusterState state, Dictionary<string, WorkerReport> nodes, bool handoff)
    {
        var replicas = state.Slots.Where(p => p.Value.Release.Name == deployment.Name).ToArray();
        int InZone(string zone) => replicas.Count(p => nodes[p.Value.Node].Zone == zone);
        int OnNode(string node) => replicas.Count(p => p.Value.Node == node);
        foreach (var (key, slot) in replicas.OrderByDescending(p => OnNode(p.Value.Node)))
        {
            var source = nodes[slot.Node];
            foreach (var target in nodes.Values.OrderBy(n => InZone(n.Zone)).ThenBy(n => OnNode(n.Node)).ThenBy(n => n.Node, StringComparer.Ordinal))
            {
                if (target.Node == source.Node || !Eligible(target, slot.Release, key) || !Fits(target, slot.Release, state.Slots.Values)) continue;
                int zoneDelta = InZone(source.Zone) - InZone(target.Zone);
                bool improvesZone = source.Zone != target.Zone && zoneDelta > 1;
                bool improvesHost = (source.Zone == target.Zone || zoneDelta == 1) && OnNode(source.Node) - OnNode(target.Node) > 1;
                if (!improvesZone && !improvesHost) continue;
                state.Slots[key] = handoff ? slot with { Stopping = true } :
                    slot with { Node = target.Node, Generation = Guid.NewGuid().ToString("N") };
                return;
            }
        }
    }

    static bool Eligible(WorkerReport worker, Deployment release, string key) =>
        worker.FailedSlots?.Contains(key, StringComparer.Ordinal) != true &&
        (worker.Images == null || worker.Images.Contains(release.Image, StringComparer.Ordinal)) &&
        (!release.Mesh || worker.Bindings.Contains(key, StringComparer.Ordinal));
    static bool Fits(WorkerReport worker, Deployment release, IEnumerable<Placement> others)
    {
        var local = others.Where(s => s.Node == worker.Node).ToArray();
        return local.Sum(s => (long)s.Release.MemoryMiB) + release.MemoryMiB <= worker.MemoryMiB &&
            local.Sum(s => (long)s.Release.MilliCpu) + release.MilliCpu <= worker.MilliCpu;
    }
}

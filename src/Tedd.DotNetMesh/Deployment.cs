using Tedd.DotNetPod;

namespace Tedd.DotNetMesh;

public sealed record Deployment(string Name, string Snapshot, string Image, string EntryPoint, int Replicas = 1,
    int MemoryMiB = 256, int MilliCpu = 1000, int Pids = 128, bool Mesh = false, string? ReadinessFile = null,
    string? LivenessFile = null, int LivenessTimeoutSeconds = 30)
{
    public void Validate()
    {
        PodSpec.ValidateId(Name);
        if (Name.Length > 32 || Replicas is < 0 or > 128) throw new ArgumentException("Invalid deployment size.");
        PodSpec.ValidateId(Snapshot);
        if (LivenessFile != null) PodSpec.ValidateMarker(LivenessFile);
        if (LivenessTimeoutSeconds is < 5 or > 3600) throw new ArgumentOutOfRangeException(nameof(LivenessTimeoutSeconds));
        Template(0, "/app/source", "/data/source", null).Validate();
    }

    public string Slot(int ordinal) => $"{Name}-{ordinal}";
    public PodSpec Template(int ordinal, string app, string data, MeshBinding? binding) => new(Slot(ordinal),
        Image, app, EntryPoint, data, MemoryMiB, MilliCpu, Pids, binding?.Socket, binding?.KeyFile, ReadinessFile: ReadinessFile, LivenessFile: LivenessFile);
}

public sealed record MeshBinding(string Socket, string KeyFile, string? Address = null);
public sealed record WorkerReport(string Node, string Zone, DateTimeOffset Seen, int MemoryMiB, int MilliCpu,
    bool Drain, string[] Bindings, Dictionary<string, string> Ready, string[]? Images = null, Dictionary<string, string>? Addresses = null,
    string[]? FailedSlots = null);
public sealed record Placement(string Node, int Ordinal, Deployment Release, string Generation);

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
        foreach (var (key, slot) in state.Slots.ToArray())
            if (!state.Deployments.TryGetValue(slot.Release.Name, out var d) || slot.Ordinal >= d.Replicas ||
                !nodes.TryGetValue(slot.Node, out var n) || !Eligible(n, slot.Release, key))
                state.Slots.Remove(key);

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
            // Change one running replica at a time and wait for its new generation to become ready.
            var mayUpdate = existing.All(s => nodes[s.Node].Ready.GetValueOrDefault(d.Slot(s.Ordinal)) == s.Generation);
            // An operator can replace a broken rollout with a different revision.
            var recovering = existing.Where(s => nodes[s.Node].Ready.GetValueOrDefault(d.Slot(s.Ordinal)) != s.Generation)
                .OrderBy(s => s.Ordinal).FirstOrDefault();
            for (int i = 0; i < d.Replicas; i++)
            {
                var key = d.Slot(i);
                state.Slots.TryGetValue(key, out var current);
                if (current != null && ((current.Release with { Replicas = d.Replicas }) == d ||
                    !mayUpdate && current != recovering)) continue;
                var candidates = nodes.Values.Where(n => Eligible(n, d, key) && Fits(n, d, state.Slots.Where(p => p.Key != key).Select(p => p.Value)));
                var selected = candidates.OrderBy(n => state.Slots.Count(p => p.Key != key && p.Value.Release.Name == d.Name && nodes[p.Value.Node].Zone == n.Zone))
                    .ThenBy(n => state.Slots.Count(p => p.Key != key && p.Value.Release.Name == d.Name && p.Value.Node == n.Node))
                    .ThenBy(n => n.Node == current?.Node ? 0 : 1)
                    .ThenBy(n => n.Node, StringComparer.Ordinal).FirstOrDefault();
                if (selected == null) continue;
                state.Slots[key] = new(selected.Node, i, d, Guid.NewGuid().ToString("N"));
                if (current != null) mayUpdate = false;
            }
            if (mayUpdate && existing.Length == d.Replicas)
                RebalanceOne(d, state, nodes);
        }
    }

    static void RebalanceOne(Deployment deployment, ClusterState state, Dictionary<string, WorkerReport> nodes)
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
                state.Slots[key] = slot with { Node = target.Node, Generation = Guid.NewGuid().ToString("N") };
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

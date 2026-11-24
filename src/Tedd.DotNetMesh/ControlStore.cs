using System.Text.Json;
using Tedd.Tdfs;

namespace Tedd.DotNetMesh;

/// <summary>Transactions on a single volume serialize deployment changes and worker heartbeats.</summary>
public sealed class ControlStore(DfsVolume volume)
{
    const string Document = "/dotnetmesh-state.json";
    public async Task<ClusterState> ReadAsync(CancellationToken ct) =>
        await volume.Files.ExistsAsync(Document, ct) ? Decode(await volume.Files.ReadAllBytesAsync(Document, ct)) : new();

    public async Task<ClusterState> ChangeAsync(Action<ClusterState> change, CancellationToken ct)
    {
        for (int attempt = 0; ; attempt++)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await using var transaction = await volume.Transactions.BeginAsync(ct: ct);
                var files = transaction.Files;
                var state = await files.ExistsAsync(Document, ct) ? Decode(await files.ReadAllBytesAsync(Document, ct)) : new();
                var previous = state.Slots.ToDictionary(p => p.Key, p => p.Value.Generation);
                change(state);
                foreach (var id in previous.Keys.Union(state.Slots.Keys))
                {
                    var generation = state.Slots.GetValueOrDefault(id)?.Generation;
                    if (previous.GetValueOrDefault(id) == generation) continue;
                    var path = ReplicaFence.PathFor(id);
                    if (generation == null) await files.DeleteAsync(path, ct);
                    else await files.WriteAllTextAsync(path, generation, ct: ct);
                }
                await files.WriteAllBytesAsync(Document, JsonSerializer.SerializeToUtf8Bytes(state), ct);
                await transaction.CommitAsync(ct);
                return state;
            }
            catch (Exception e) when (attempt < 12 && e is DfsTransactionConflictException or DfsAlreadyExistsException or DfsSharingViolationException)
            {
                await Task.Delay(Random.Shared.Next(5, Math.Min(250, 10 << Math.Min(attempt, 5))), ct);
            }
        }
    }

    static ClusterState Decode(byte[] bytes) => JsonSerializer.Deserialize<ClusterState>(bytes) ?? throw new InvalidDataException("Empty control state.");
}

using Tedd.DotNetPod;
using Tedd.Tdfs;

namespace Tedd.DotNetMesh;

/// <summary>Every writable mount operation reads its allocation generation in the same serializable transaction.</summary>
public sealed class ReplicaFence(DfsVolume volume, string slot, string generation)
{
    internal static string PathFor(string slot)
    {
        PodSpec.ValidateId(slot);
        return "/dotnetmesh-fence-" + slot;
    }

    public T Mutate<T>(Func<DfsFileClient, DfsDirectoryClient, T> mutation)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                using var tx = volume.Transactions.Begin();
                var path = PathFor(slot);
                if (!tx.Files.Exists(path) || tx.Files.ReadAllText(path) != generation)
                    throw new UnauthorizedAccessException("The replica no longer owns its data mount.");
                var result = mutation(tx.Files, tx.Directories);
                tx.Commit();
                return result;
            }
            catch (DfsTransactionConflictException) when (attempt < 3) { }
        }
    }
}

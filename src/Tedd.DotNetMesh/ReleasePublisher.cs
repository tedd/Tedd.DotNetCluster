using Tedd.Tdfs;

namespace Tedd.DotNetMesh;

public static class ReleasePublisher
{
    /// <summary>Installs a complete release and names its volume snapshot in the same TDFS commit.</summary>
    public static async Task PublishAsync(DfsVolume volume, Deployment release, string directory, CancellationToken ct)
    {
        release.Validate();
        var source = new DirectoryInfo(Path.GetFullPath(directory));
        if (!source.Exists || !File.Exists(Path.Combine(source.FullName, release.EntryPoint)))
            throw new FileNotFoundException("Published directory must contain the declared entry point.");
        await using var tx = await volume.Transactions.BeginAsync(ct: ct);
        var root = "/apps/" + release.Name;
        if (await tx.Directories.ExistsAsync(root, ct)) await tx.Directories.DeleteAsync(root, true, ct);
        await CopyDirectory(source, root);
        await tx.CommitWithSnapshotAsync(release.Snapshot, ct);

        async Task CopyDirectory(DirectoryInfo from, string target)
        {
            RejectLink(from);
            await tx.Directories.CreateDirectoryAsync(target, ct);
            foreach (var item in from.EnumerateFileSystemInfos())
            {
                ct.ThrowIfCancellationRequested();
                RejectLink(item);
                var path = VolumeMount.ScopedPath(target, "/" + item.Name);
                if (item is DirectoryInfo subdirectory) { await CopyDirectory(subdirectory, path); continue; }
                await using var input = new FileStream(item.FullName, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, true);
                await using var output = await tx.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write }, ct);
                await input.CopyToAsync(output, ct);
            }
        }

        static void RejectLink(FileSystemInfo entry)
        {
            if ((entry.Attributes & FileAttributes.ReparsePoint) != 0) throw new InvalidDataException("Release trees must contain regular files and directories.");
        }
    }
}

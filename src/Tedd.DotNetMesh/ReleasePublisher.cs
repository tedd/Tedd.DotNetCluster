using System.IO.Compression;
using System.IO.Hashing;
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
        await PublishTreeAsync(volume, release, (tx, root) => CopyDirectory(tx, source, root), ct);

        async Task CopyDirectory(DfsTransaction tx, DirectoryInfo from, string target)
        {
            RejectLink(from);
            await tx.Directories.CreateDirectoryAsync(target, ct);
            foreach (var item in from.EnumerateFileSystemInfos())
            {
                ct.ThrowIfCancellationRequested();
                RejectLink(item);
                var path = VolumeMount.ScopedPath(target, "/" + item.Name);
                if (item is DirectoryInfo subdirectory) { await CopyDirectory(tx, subdirectory, path); continue; }
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

    internal static async Task PublishZipAsync(DfsVolume volume, Deployment release, Stream input, CancellationToken ct)
    {
        using var zip = new ZipArchive(input, ZipArchiveMode.Read, leaveOpen: true);
        const long maxExpandedBytes = 1024L * 1024 * 1024;
        if (zip.Entries.Count > 16_384) throw new InvalidDataException("Release ZIP exceeds the entry limit.");
        var paths = new HashSet<string>(StringComparer.Ordinal);
        long remaining = maxExpandedBytes;
        foreach (var entry in zip.Entries)
        {
            var directory = entry.FullName.EndsWith('/');
            var name = directory ? entry.FullName[..^1] : entry.FullName;
            var unixType = (entry.ExternalAttributes >> 16) & 0xf000;
            if (name.Length == 0 || unixType != 0 && unixType != (directory ? 0x4000 : 0x8000) ||
                (entry.ExternalAttributes & (int)FileAttributes.ReparsePoint) != 0 || directory && entry.Length != 0)
                throw new InvalidDataException("Release ZIP must contain regular files and directories.");
            var path = VolumeMount.ScopedPath("/apps/" + release.Name, "/" + name);
            if (DfsPath.Normalize(path) != path || !paths.Add(path))
                throw new InvalidDataException($"Duplicate or noncanonical ZIP entry: {name}");
            if (entry.Length < 0 || entry.Length > remaining) throw new InvalidDataException("Expanded ZIP exceeds 1 GiB.");
            remaining -= entry.Length;
        }
        if (zip.GetEntry(release.EntryPoint) == null) throw new FileNotFoundException(release.EntryPoint);
        await PublishTreeAsync(volume, release, async (tx, root) =>
        {
            var directories = new HashSet<string>(StringComparer.Ordinal);
            var buffer = new byte[65536];
            foreach (var entry in zip.Entries)
            {
                var path = root + "/" + entry.FullName;
                var parent = path[..path.LastIndexOf('/')];
                if (directories.Add(parent)) await tx.Directories.CreateDirectoryAsync(parent, ct);
                if (entry.FullName.EndsWith('/')) continue;
                await using var source = await entry.OpenAsync(ct);
                await using var output = await tx.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write }, ct);
                var crc = new Crc32();
                long copied = 0;
                int read;
                while ((read = await source.ReadAsync(buffer, ct)) != 0)
                {
                    copied += read;
                    if (copied > entry.Length) throw new InvalidDataException("ZIP entry exceeds its declared size.");
                    crc.Append(buffer.AsSpan(0, read));
                    await output.WriteAsync(buffer.AsMemory(0, read), ct);
                }
                if (copied != entry.Length || crc.GetCurrentHashAsUInt32() != entry.Crc32)
                    throw new InvalidDataException($"Incomplete or corrupt ZIP entry: {entry.FullName}");
            }
        }, ct);
    }

    static async Task PublishTreeAsync(DfsVolume volume, Deployment release, Func<DfsTransaction, string, Task> copy, CancellationToken ct)
    {
        await using var tx = await volume.Transactions.BeginAsync(ct: ct);
        var root = "/apps/" + release.Name;
        if (await tx.Directories.ExistsAsync(root, ct)) await tx.Directories.DeleteAsync(root, true, ct);
        await copy(tx, root);
        foreach (var mountPoint in release.ApplicationMountPoints())
            await tx.Directories.CreateDirectoryAsync(mountPoint, ct);
        await release.ValidateApplicationAsync(tx.Files, tx.Directories, ct);
        await tx.CommitWithSnapshotAsync(release.Snapshot, ct);
    }
}

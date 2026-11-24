using System.Runtime.InteropServices;
using Tedd.Tdfs;

namespace Tedd.DotNetMesh;

/// <summary>One FUSE superblock exports exactly one TDFS subtree. The single FUSE loop serializes handle access.</summary>
public sealed class VolumeMount : IDisposable
{
    readonly DfsFileClient files;
    readonly DfsDirectoryClient dirs;
    readonly string root;
    readonly ReplicaFence? fence;
    readonly Dictionary<ulong, DfsStream> handles = new();
    readonly Native.Callback callback;
    readonly nint mount;
    readonly Task<int> loop;
    ulong next;
    int disposed;

    public VolumeMount(string target, string subtree, DfsFileClient files, DfsDirectoryClient dirs, ReplicaFence? fence = null)
    {
        if (!OperatingSystem.IsLinux()) throw new PlatformNotSupportedException("A Linux FUSE3 worker is required.");
        this.files = files; this.dirs = dirs; root = subtree; this.fence = fence;
        if (!files.IsReadOnly && fence == null) throw new ArgumentException("Writable mounts require an allocation fence.");
        if (ScopedPath(root, "/") != root || !dirs.Exists(root)) throw new DirectoryNotFoundException(root);
        callback = Dispatch;
        mount = Native.mesh_mount(target, callback);
        if (mount == 0) throw new IOException($"Unable to mount TDFS at {target}.");
        loop = Task.Factory.StartNew(() => Native.mesh_loop(mount), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    public static string ScopedPath(string root, string path)
    {
        if (!Valid(root) || !Valid(path)) throw new ArgumentException("Invalid TDFS subtree path.");
        return path == "/" ? root : root == "/" ? path : root + path;
        static bool Valid(string s) => s == "/" || s is { Length: > 1 } && s.StartsWith('/') && !s.Contains(':') &&
            !s.Contains('\\') && !s.Any(char.IsControl) && s.Split('/').Skip(1).All(c => c is not ("" or "." or ".."));
    }

    enum Op { Attr, List, Open, Create, Read, Write, Flush, Release, Mkdir, Unlink, Rmdir, Rename, Truncate }

    unsafe long Dispatch(int operation, string relative, string? second, ulong handle, long offset, nint buffer, int count)
    {
        try
        {
            var op = (Op)operation;
            var path = ScopedPath(root, relative);
            handles.TryGetValue(handle, out var stream);
            switch (op)
            {
                case Op.Attr:
                    long* meta = (long*)buffer;
                    meta[3] = files.IsReadOnly ? 1 : 0;
                    if (stream != null) { meta[1] = stream.Length; return 0; }
                    if (dirs.Exists(path))
                    {
                        meta[0] = 1;
                        meta[2] = dirs.GetInfo(path).ModifiedUtc.ToUnixTimeSeconds();
                    }
                    else
                    {
                        var info = files.GetInfo(path);
                        meta[1] = info.Length; meta[2] = info.ModifiedUtc.ToUnixTimeSeconds();
                    }
                    return 0;
                case Op.List:
                    long index = 0;
                    foreach (var name in new[] { ".", ".." }.Concat(dirs.Enumerate(path).Select(e => e.Name)))
                        if (++index > offset && Native.mesh_fill(buffer, name, index) != 0) break;
                    return 0;
                case Op.Open:
                case Op.Create:
                    var access = (offset & 3) switch { 0 => FileAccess.Read, 1 => FileAccess.Write, _ => FileAccess.ReadWrite };
                    if (files.IsReadOnly && (op == Op.Create || access != FileAccess.Read)) return -30;
                    var mode = op == Op.Create ? ((offset & 128) != 0 ? FileMode.CreateNew : FileMode.OpenOrCreate) : FileMode.Open;
                    if (op == Op.Create || (offset & 512) != 0)
                        fence!.Mutate((f, _) =>
                        {
                            using var created = f.Open(path, Writer(mode));
                            if ((offset & 512) != 0) created.SetLength(0);
                            return 0;
                        });
                    var opened = files.Open(path, new DfsOpenOptions { Share = FileShare.ReadWrite | FileShare.Delete });
                    handles.Add(++next, opened);
                    return checked((long)next);
                case Op.Read:
                    if (stream == null) return -9;
                    stream.Position = offset;
                    return stream.Read(new Span<byte>((void*)buffer, count));
                case Op.Write:
                    if (stream == null) return -9;
                    if (fence == null) return -30;
                    return fence.Mutate((f, _) =>
                    {
                        using var writer = f.Open(path, Writer(FileMode.Open));
                        if (writer.InodeId != stream.InodeId) throw new IOException("Open inode was replaced.");
                        writer.Position = offset;
                        writer.Write(new ReadOnlySpan<byte>((void*)buffer, count));
                        return count;
                    });
                case Op.Flush:
                    if (stream?.CanWrite == true) stream.Flush();
                    return 0;
                case Op.Release:
                    if (handles.Remove(handle, out var closed)) closed.Dispose();
                    return 0;
            }
            if (files.IsReadOnly) return -30;
            if (path == root && op != Op.Truncate) return -16;
            return fence!.Mutate((f, d) =>
            {
                switch (op)
                {
                    case Op.Mkdir:
                        if (d.Exists(path)) return -17;
                        d.CreateDirectory(path); break;
                    case Op.Unlink: f.Delete(path); break;
                    case Op.Rmdir: d.Delete(path); break;
                    case Op.Rename:
                        var dest = ScopedPath(root, second!);
                        if (dest == root) return -16;
                        if (d.Exists(path)) d.Move(path, dest);
                        else f.Move(path, dest, overwrite: offset == 0);
                        break;
                    case Op.Truncate:
                        using (var writer = f.Open(path, Writer(FileMode.Open)))
                        {
                            if (stream != null && stream.InodeId != writer.InodeId) throw new IOException("Open inode was replaced.");
                            writer.SetLength(offset);
                        }
                        break;
                    default: return -38;
                }
                return 0;
            });
        }
        catch (Exception error)
        {
            return error switch
            {
                FileNotFoundException or DirectoryNotFoundException => -2,
                DfsAlreadyExistsException => -17,
                DfsDirectoryNotEmptyException => -39,
                DfsReadOnlyException => -30,
                UnauthorizedAccessException => -13,
                ArgumentException => -22,
                DfsSharingViolationException => -16,
                DfsCapacityException => -28,
                _ => -5
            };
        }
    }

    static DfsOpenOptions Writer(FileMode mode) => new() { Mode = mode, Access = FileAccess.Write, Share = FileShare.Read | FileShare.Delete };

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0) return;
        Native.mesh_stop(mount);
        loop.GetAwaiter().GetResult();
        Native.mesh_destroy(mount);
        foreach (var stream in handles.Values) stream.Dispose();
        GC.KeepAlive(callback);
    }

    static class Native
    {
        const string Lib = "dotnetmesh_fuse";
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate long Callback(int op, [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string? second, ulong handle, long offset, nint buffer, int count);
        [DllImport(Lib)] internal static extern nint mesh_mount([MarshalAs(UnmanagedType.LPUTF8Str)] string target, Callback callback);
        [DllImport(Lib)] internal static extern int mesh_fill(nint buffer, [MarshalAs(UnmanagedType.LPUTF8Str)] string name, long next);
        [DllImport(Lib)] internal static extern int mesh_loop(nint mount);
        [DllImport(Lib)] internal static extern void mesh_stop(nint mount);
        [DllImport(Lib)] internal static extern void mesh_destroy(nint mount);
    }
}

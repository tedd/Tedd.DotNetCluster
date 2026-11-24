using System.Net.Sockets;

namespace Tedd.DotNetMesh;

/// <summary>A worker-owned Unix socket grants mesh access for the lifetime of an assignment.</summary>
internal sealed class AllocationSocket : IAsyncDisposable
{
    readonly string path, upstream;
    readonly Socket listener = new(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    readonly CancellationTokenSource stop = new();
    readonly HashSet<Task> sessions = new();
    readonly Task accept;
    readonly ITimer expiry;
    public bool Expired => stop.IsCancellationRequested;

    public AllocationSocket(string path, string upstream, TimeProvider clock)
    {
        if (!OperatingSystem.IsLinux()) throw new PlatformNotSupportedException();
        this.path = path; this.upstream = upstream;
        try
        {
            listener.Bind(new UnixDomainSocketEndPoint(path));
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupWrite | UnixFileMode.OtherWrite);
            listener.Listen(64);
            expiry = clock.CreateTimer(_ => stop.Cancel(), null, Scheduler.FailureTimeout, Timeout.InfiniteTimeSpan);
            accept = AcceptAsync();
        }
        catch { listener.Dispose(); throw; }
    }

    public void Renew() => expiry.Change(Scheduler.FailureTimeout, Timeout.InfiniteTimeSpan);
    public void Revoke() => expiry.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);

    async Task AcceptAsync()
    {
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var socket = await listener.AcceptAsync(stop.Token);
                sessions.RemoveWhere(s => s.IsCompleted);
                if (sessions.Count >= 256) { socket.Dispose(); continue; }
                sessions.Add(ForwardAsync(socket));
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }

    async Task ForwardAsync(Socket socket)
    {
        using var local = socket;
        using var remote = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        using var ends = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
        try
        {
            await remote.ConnectAsync(new UnixDomainSocketEndPoint(upstream), ends.Token);
            using var a = new NetworkStream(local);
            using var b = new NetworkStream(remote);
            await Task.WhenAll(Pump(a, b, remote), Pump(b, a, local));

            async Task Pump(Stream from, Stream to, Socket peer)
            {
                try { await from.CopyToAsync(to, 16384, ends.Token); peer.Shutdown(SocketShutdown.Send); }
                catch { await ends.CancelAsync(); throw; }
            }
        }
        catch (Exception e) when (e is OperationCanceledException or IOException or SocketException or ObjectDisposedException) { }
    }

    public async ValueTask DisposeAsync()
    {
        await expiry.DisposeAsync();
        await stop.CancelAsync();
        try { await accept; await Task.WhenAll(sessions); }
        finally { listener.Dispose(); stop.Dispose(); File.Delete(path); }
    }
}

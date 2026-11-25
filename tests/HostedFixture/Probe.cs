using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace HostedFixture;

public sealed record Journal(string Path);

public sealed class JournalModule : Tedd.DotNetMesh.ServiceHost.IServiceModule
{
    public void Configure(IHostApplicationBuilder builder) =>
        builder.Services.AddSingleton(new Journal(builder.Configuration["Journal"]!));
}

public sealed class Probe(Journal journal, IHostApplicationLifetime lifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await File.AppendAllTextAsync(journal.Path, "started\n", stoppingToken);
        lifetime.ApplicationStarted.Register(lifetime.StopApplication);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        await File.AppendAllTextAsync(journal.Path, "stopped\n", cancellationToken);
    }
}

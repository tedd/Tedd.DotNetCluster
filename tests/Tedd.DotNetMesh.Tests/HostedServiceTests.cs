using System.Diagnostics;
using System.Text.Json;
using HostedFixture;
using Tedd.DotNetMesh.ServiceHost;

namespace Tedd.DotNetMesh.Tests;

public class HostedServiceTests
{
    [Fact]
    public async Task RunnerStartsAndStopsAServiceLibraryWithInjectedDependencies()
    {
        var ct = TestContext.Current.CancellationToken;
        var release = Directory.CreateTempSubdirectory("servicehost-");
        try
        {
            var library = release.CreateSubdirectory("services");
            var host = typeof(ServiceWorkload).Assembly.Location;
            foreach (var extension in new[] { ".dll", ".deps.json", ".runtimeconfig.json" })
            {
                var source = Path.ChangeExtension(host, extension);
                File.Copy(source, Path.Combine(release.FullName, Path.GetFileName(source)));
            }
            var fixture = typeof(JournalModule).Assembly.Location;
            File.Copy(fixture, Path.Combine(library.FullName, Path.GetFileName(fixture)));
            var journal = Path.Combine(release.FullName, "run.log");
            var settings = new
            {
                ServiceHost = new { Assembly = "services/HostedFixture.dll", Types = new[] { "HostedFixture.JournalModule", "HostedFixture.Probe" } }
            };
            await File.WriteAllTextAsync(Path.Combine(release.FullName, "appsettings.json"), JsonSerializer.Serialize(settings), ct);

            var info = new ProcessStartInfo("dotnet") { WorkingDirectory = release.FullName, RedirectStandardError = true };
            info.Environment["DOTNET_ENVIRONMENT"] = "Testing";
            info.ArgumentList.Add(Path.GetFileName(host));
            info.ArgumentList.Add("--Journal");
            info.ArgumentList.Add(journal);
            using var process = Process.Start(info)!;
            var errors = process.StandardError.ReadToEndAsync(ct);
            try
            {
                await process.WaitForExitAsync(ct).WaitAsync(TimeSpan.FromSeconds(20), ct);
                Assert.True(process.ExitCode == 0, await errors);
                Assert.Equal(new[] { "started", "stopped" }, await File.ReadAllLinesAsync(journal, ct));
            }
            finally
            {
                if (!process.HasExited) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(CancellationToken.None); }
            }
        }
        finally { release.Delete(recursive: true); }
    }
}

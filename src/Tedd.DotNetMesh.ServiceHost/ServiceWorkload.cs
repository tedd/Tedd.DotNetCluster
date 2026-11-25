using System.Reflection;
using System.Runtime.Loader;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Tedd.DotNetMesh.ServiceHost;

/// <summary>Registers a workload's dependencies and hosted services before the host is built.</summary>
public interface IServiceModule
{
    void Configure(IHostApplicationBuilder builder);
}

public static class ServiceWorkload
{
    public static void Configure(IHostApplicationBuilder builder)
    {
        var section = builder.Configuration.GetRequiredSection("ServiceHost");
        var file = section["Assembly"];
        if (file == null || !file.EndsWith(".dll", StringComparison.OrdinalIgnoreCase) ||
            file.Contains('\\') || file.Contains(':') || file.Any(char.IsControl) ||
            file.Split('/').Any(segment => segment is "" or "." or ".."))
            throw new InvalidDataException("ServiceHost.Assembly must be a DLL path relative to the release root.");
        var names = section.GetSection("Types").Get<string[]>();
        if (names is not { Length: > 0 } || names.Any(string.IsNullOrWhiteSpace) ||
            names.Distinct(StringComparer.Ordinal).Count() != names.Length)
            throw new InvalidDataException("ServiceHost.Types must list distinct service or module type names.");

        var path = Path.Combine(builder.Environment.ContentRootPath, file);
        var assembly = new WorkloadContext(path).LoadFromAssemblyPath(path);
        foreach (var name in names)
        {
            var type = assembly.GetType(name, throwOnError: true)!;
            if (!type.IsClass || type.IsAbstract || type.ContainsGenericParameters)
                throw new InvalidDataException($"{name} must be a concrete service or module.");
            if (typeof(IServiceModule).IsAssignableFrom(type))
                ((IServiceModule)Activator.CreateInstance(type)!).Configure(builder);
            else if (typeof(IHostedService).IsAssignableFrom(type))
                builder.Services.AddSingleton(typeof(IHostedService), type);
            else
                throw new InvalidDataException($"{name} must implement IHostedService or IServiceModule.");
        }
    }

    sealed class WorkloadContext(string assemblyPath) : AssemblyLoadContext("workload")
    {
        readonly AssemblyDependencyResolver resolver = new(assemblyPath);

        // The runner and workload must agree on the identity of shared framework and host contracts.
        readonly HashSet<string> shared = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator).Select(path => Path.GetFileNameWithoutExtension(path)!)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        protected override Assembly? Load(AssemblyName name)
        {
            if (shared.Contains(name.Name!)) return Default.LoadFromAssemblyName(name);
            var path = resolver.ResolveAssemblyToPath(name);
            return path == null ? null : LoadFromAssemblyPath(path);
        }

        protected override nint LoadUnmanagedDll(string name)
        {
            var path = resolver.ResolveUnmanagedDllToPath(name);
            return path == null ? 0 : LoadUnmanagedDllFromPath(path);
        }
    }
}

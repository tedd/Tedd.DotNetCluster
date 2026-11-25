using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Tedd.DotNetMesh.ServiceHost;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});
builder.Services.AddHttpClient();
ServiceWorkload.Configure(builder);
await builder.Build().RunAsync();

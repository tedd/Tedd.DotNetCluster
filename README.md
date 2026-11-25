# Tedd.DotNetCluster

Tedd.DotNetCluster runs replicated .NET services on Linux workers. Publish a .NET application once to a TDFS snapshot, then declare the snapshot, runtime image, resource limits and replica count. Workers place replicas across eligible hosts and zones using DotNetPod containers. Each replica gets a read-only application at `/app` and its own persistent files at `/data`.

[Project website](https://tedd.no/Tedd.DotNetCluster/) · [Service specification](SPEC.md) · [Apache-2.0 license](LICENSE)

## Run the sample

The included `MeshFixture` service increments a counter in `/data/files/starts` and creates `/tmp/ready` after startup. This walkthrough runs it on one Linux worker with loopback networking. The repository builds the worker and CLI as `Tedd.DotNetMesh.dll`.

### 1. Prepare the worker

- Use the .NET 11 SDK (11.0.100 or a compatible 11.0 feature band), a C compiler, FUSE 3 headers and libraries, and a local Linux Docker Engine with resource controls enabled. The worker needs access to the engine socket and `/dev/fuse`, permission to mount with `allow_other`, and the MsQuic native library on its loader path.
- Provision a TDFS volume and a principal with access to read and write its control state and data, publish releases and open snapshots. Obtain the cluster ID, QUIC seed endpoints and trusted server public-key pins from the cluster administrator. See the [TDFS operations guide](https://github.com/tedd/Tedd.TDFS/blob/main/docs/OPERATIONS.md).
- Place the source directories `Tedd.DotNetPod`, `Tedd.HostNetMesh`, `Tedd.SMB3` and `Tedd.TDFS` alongside this checkout. The project references these exact sibling paths. Run the following commands in Bash from the root of this repository.

```bash
dotnet publish src/Tedd.DotNetMesh -c Release -o ./out/worker
cc -O2 -Wall -Wextra -Werror -fPIC -shared native/tdfs_mount.c -lfuse3 \
  -o ./out/worker/libdotnetmesh_fuse.so
dotnet publish tests/MeshFixture -c Release --no-self-contained -o ./out/sample
docker pull mcr.microsoft.com/dotnet/runtime:11.0
docker image inspect --format '{{.Id}}' mcr.microsoft.com/dotnet/runtime:11.0
```

### 2. Configure storage and placement

Save the following as `cluster.json`, replacing the example connection values with your TDFS settings. Put the principal's secret in `SecretFile`. Give the worker a unique node name and a private, writable runtime directory on a local filesystem visible to the engine. An empty `Bindings` object is sufficient for this sample.

```json
{
  "ClusterId": "00000000-0000-0000-0000-000000000001",
  "Seeds": ["quic://tdfs.example.net:7100"],
  "Volume": "services",
  "Principal": "worker-a",
  "SecretFile": "/etc/dotnetmesh/tdfs-secret",
  "SpkiPins": ["<base64 SHA-256 digest of the server SPKI>"],
  "Worker": {
    "Node": "worker-a",
    "Zone": "east",
    "RuntimeDirectory": "/var/lib/dnm/a",
    "MemoryMiB": 2048,
    "MilliCpu": 2000,
    "Bindings": {}
  }
}
```

Save this deployment as `sample.json`. Replace the entire `Image` value with the local `sha256:…` image ID printed by Docker. Each eligible worker must have that image installed.

```json
{
  "Name": "sample",
  "Snapshot": "sample-r1",
  "Image": "<local Docker image ID>",
  "EntryPoint": "MeshFixture.dll",
  "Replicas": 1,
  "MemoryMiB": 256,
  "MilliCpu": 500,
  "ReadinessFile": "ready"
}
```

### 3. Publish, apply and verify

```bash
dotnet ./out/worker/Tedd.DotNetMesh.dll publish cluster.json sample.json ./out/sample
dotnet ./out/worker/Tedd.DotNetMesh.dll apply cluster.json sample.json
dotnet ./out/worker/Tedd.DotNetMesh.dll worker cluster.json
```

`publish` stores the application and names its snapshot. `apply` sets the desired deployment. Leave the worker running and inspect the cluster from a second terminal in the repository root.

```bash
dotnet ./out/worker/Tedd.DotNetMesh.dll status cluster.json
```

Find `sample-0` in `Slots`. Its `Node` identifies the assigned worker. Once ready, that worker's `Nodes.<node>.Ready["sample-0"]` matches the slot's `Generation` and its `Seen` timestamp is current. If the slot is absent, check the worker heartbeat, reported image IDs and available CPU and memory.

## Operate a deployment

| Task | Procedure |
| --- | --- |
| Scale | Change `Replicas` and run `apply` again. Each ordinal (such as `sample-0`) retains its own data when moved or scaled down. |
| Upgrade | Publish the new application under a fresh `Snapshot` name and apply its manifest. Workers normally replace one replica at a time and wait for readiness before proceeding. A single replica has downtime during replacement. |
| Roll back | Apply the previous manifest while its snapshot is retained and its runtime image is installed. Rollback selects the old application files. Persistent data keeps its current contents. |
| Deploy a GitHub release | Run `dotnet ./out/worker/Tedd.DotNetMesh.dll github-latest cluster.json sample.json OWNER/REPO asset.zip`. The ZIP must contain the published files with the entry DLL at its root. The command publishes and applies the release, then prints its snapshot name. |
| Remove | Run `dotnet ./out/worker/Tedd.DotNetMesh.dll delete cluster.json sample`. Workers remove its containers. The replica data remains on TDFS. |

## Networking and integrations

Enable `Mesh` and configure a HostMesh identity for each eligible replica slot to give applications scoped network access. A separate `gateway` process can route TCP connections to ready replicas. Linux workers can also expose resident replicas' data over encrypted SMB 3.1.1 shares. For class libraries implementing `IHostedService` or `BackgroundService`, the supplied ServiceHost runner loads the configured service types.

See the [gateway configuration](SPEC.md#service-gateway), [SMB share permissions](SPEC.md#host-smb-shares), [Generic Host instructions](SPEC.md#generic-host-services) and [availability contract](SPEC.md#availability-contract).

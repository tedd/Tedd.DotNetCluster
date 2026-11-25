# Tedd.DotNetMesh service specification

DotNetMesh runs replicated .NET 11 services on Linux workers. A deployment names an application snapshot, an immutable runtime image, resource requests and a replica count. Workers reconcile desired state through TDFS, launch isolated instances through DotNetPod and connect applications to HostMesh through scoped Unix sockets.

## Availability contract

| Dependency | Requirement | Behavior during failure |
| --- | --- | --- |
| Control plane | A reachable TDFS volume metadata majority and current worker heartbeats | Existing assignments converge after quorum returns. A worker that loses contact withdraws local workloads after its 30-second lease expires and reconciliation runs. Mesh proxies have independent expiry timers. |
| Compute | Sufficient CPU, memory, healthy engines and the requested image on eligible workers | The scheduler assigns each ordinal to one worker. It spreads placements across zones and hosts. Excess replicas remain pending. |
| Files | Sufficient live TDFS hosts and zones for the configured volume policy | File mutations complete after a TDFS transaction commits. A stale allocation loses write authority when its replacement generation commits. |
| Network | A reachable HostMesh path, matching application grants, accessible local agents and a ready service endpoint | HostMesh can reroute an open stream around a failed relay. A process or gateway failure closes streams it terminated. New connections select a ready endpoint. |

A replacement retains its ordinal's data. Applications must make retried external side effects idempotent. Container execution can overlap during a partition. TDFS fences file mutations, and the worker proxy limits the lifetime of mesh credentials in use. Storage replication, worker placement and gateway deployment are separate redundancy decisions. Maintain synchronized worker clocks with skew small relative to the heartbeat timeout.

## Control plane and placement

Each worker publishes CPU, memory, zone, installed image IDs, available HostMesh bindings, ready generations and last contact. A two-second heartbeat proceeds independently of local container operations. Graceful shutdown announces a drain. Surviving workers expire a silent node after 30 seconds. Node names must be unique cluster-wide. Give each worker a private, persistent runtime directory and a dedicated DotNetPod scope named for the worker.

Workers serialize changes to the deployment and placement document using TDFS transactions. Each placement change writes its generation fence in the same commit. A bounded retry handles transaction conflicts. A fresh worker reconstructs desired state from TDFS, stops orphaned containers in its scope and detaches its disconnected FUSE mounts.

The scheduler keeps healthy assignments, spreads new replicas across available zones, respects reserved memory and CPU, and can redistribute a ready replica when capacity joins. It replaces one replica per deployment at a time during an ordinary update. The next replacement waits for readiness of the assigned generation. Reapplying a prior snapshot can replace a failed update. Crash exits use the engine restart policy. After three consecutive local provisioning failures, that worker withdraws the affected slot for 60 seconds so another eligible worker can take it. With a single replica, stop-first updates incur downtime. These behaviors draw on Kubernetes Deployment and topology spread semantics. [Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) · [Topology spread constraints](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/)

During a planned upgrade, a stopping replica leaves gateway rotation and keeps write access while its services shut down. The worker confirms container removal before the scheduler transfers write authority and starts the replacement. This handoff requires all live workers to support stop acknowledgements. During a mixed-version worker upgrade, placement uses the previous immediate fencing behavior. An expired worker lease also allows recovery without a stop acknowledgement.

## Filesystem

A release publication streams a local distribution tree to /apps/<deployment> and commits a named TDFS volume snapshot atomically. Use a unique snapshot name per publication. Retain snapshots for every active or rollback revision. The worker pins the named snapshot and mounts its application subtree read-only at /app. Each ordinal gets a separate writable mount backed by /data/<deployment>/<ordinal> in the live volume. Scaling down and deleting deployments retain data.

`PersistentDirectories` maps subdirectories of that ordinal's data to absolute Linux paths inside the pod. For example, `{"Source":"logs","Target":"/app/logs"}` directs relative writes to `logs/` from the application's working directory into `/data/logs`. Both paths address the same files. Changing the snapshot or target path preserves the files as long as the deployment name, ordinal and source path stay the same. Publication creates empty mount points beneath `/app`. Applying an existing snapshot requires those mount points to exist. Writable mounts hide any release content at their target. Applications can use ordinary `System.IO` APIs.

The Linux FUSE3 bridge provides directory enumeration, regular-file creation, random reads and writes, truncation, flush, rename and deletion. Both the container bind mount and the application facade enforce snapshot immutability. Each data mutation checks the generation in the same serializable transaction. The host checks inode identity before writing through an open file descriptor. Workloads that require POSIX record locks, hard links, extended attributes, device nodes, exact POSIX mode persistence or writable memory mapping require a separate filesystem compatibility assessment.

TDFS controls metadata voting, target copies, durable copies and durable zones before acknowledgement. Place its voters and storage hosts across independent failure domains. Account for the surviving capacity needed to satisfy the write acknowledgement policy after a failure. Application, data and control records use the designated volume. Workers are trusted infrastructure principals. Workloads receive access only to their mounted subtrees.

## Networking and discovery

An isolated container has loopback plus the optional HostMesh application socket and key. Configure one distinct HostMesh application identity per worker and eligible replica slot. Declare its TCP/UDP ACLs, address bindings and forwards in the HostMesh agent. DotNetMesh checks local socket reachability and mounts one forwarding socket at /mesh/agent.sock and the corresponding key at /mesh/key. Network-enabled applications can use WorkloadNetwork.Open() or construct a LocalMeshClient from those paths. Agent and worker restarts close their local sessions.

A service gateway exposes stable TCP listener ports on a separate HostMesh identity. It refreshes the deployment directory every second, balances new connections across ready generations, and tries a different eligible endpoint when connection setup fails. It copies streams in both directions and preserves half-close. The destination receives the original effective client endpoint through HostMesh's authenticated origin-forwarding operation. Grant the gateway identity permission to attest origins on an ingress-authorized node. Restrict its outgoing destinations and accepted sources to its service routes. To expose an external port, point a HostMesh physical TCP ingress at the gateway's mesh address and listener port. Direct UDP services use HostMesh datagram bindings and configured UDP forwards.

Deploy multiple gateway identities and make them reachable through the ingress layer when the gateway itself must survive a host failure. Each running TCP session remains tied to the gateway and application processes that accepted it.

## Build and run

Keep Tedd.DotNetMesh, Tedd.DotNetPod, Tedd.HostNetMesh, Tedd.SMB3 and Tedd.TDFS in sibling checkouts. Build with the .NET 11 SDK. Linux workers need libfuse3, the FUSE device, a local Linux Docker Engine with resource controls enabled, and permission to mount with allow_other. Compile the native bridge with a C compiler and the fuse3 development package. Place the built shared object beside the published worker DLL.

```sh
dotnet publish src/Tedd.DotNetMesh -c Release -o /srv/dotnetmesh/bin
cc -O2 -Wall -Wextra -Werror -fPIC -shared native/tdfs_mount.c -lfuse3 -o /srv/dotnetmesh/bin/libdotnetmesh_fuse.so
```

Install the same trusted .NET 11 runtime image on eligible workers. Obtain the local sha256 image ID with docker image inspect. Use a TDFS volume with explicit durability and ACL policies. Configure the expected cluster ID, pinned QUIC seeds and principal secret file. The following worker configuration preauthorizes one replica. Create the runtime directory on a local Linux filesystem visible to the engine. Use short paths for Unix-domain sockets. Protect the engine socket, worker directory, configuration and key files. The container UID 1654 must be able to read its mounted application key.

```json
{
  "ClusterId": "00000000-0000-0000-0000-000000000001",
  "Seeds": ["quic://10.20.0.1:7100", "quic://10.20.0.2:7100"],
  "Volume": "services",
  "Principal": "worker-a",
  "SecretFile": "/etc/dotnetmesh/tdfs-secret",
  "SpkiPins": ["<base64 SHA-256 SPKI digest>"],
  "Worker": {
    "Node": "a", "Zone": "east", "RuntimeDirectory": "/var/lib/dnm/a",
    "MemoryMiB": 4096, "MilliCpu": 4000,
    "Bindings": {
      "web-0": {"Socket": "/run/hostmesh/web-0.sock", "KeyFile": "/etc/dotnetmesh/web-0.key", "Address": "10.84.1.10"}
    }
  }
}
```

A deployment document uses the following fields. Publish a Linux-compatible .NET application directory with Web.dll at its root. When ReadinessFile is present, the application creates /tmp/ready after its listeners and dependencies are available. It removes the file when it loses readiness. The runtime image must provide /usr/bin/test for this probe.

For liveness supervision, set LivenessFile to a second marker name and LivenessTimeoutSeconds to 5–3600 seconds. The program updates this file under /tmp while it is making progress. The runtime image must provide /bin/sh and /usr/bin/stat for sampling the marker. After the startup grace interval, the worker restarts an instance whose marker is missing or older than the configured timeout. Readiness controls service selection.

```json
{
  "Name": "web", "Snapshot": "web-r42",
  "Image": "sha256:<64 lowercase hexadecimal digits>",
  "EntryPoint": "Web.dll", "Replicas": 1, "Mesh": true,
  "MemoryMiB": 256, "MilliCpu": 500, "Pids": 128, "ReadinessFile": "ready",
  "PersistentDirectories": [
    {"Source": "logs", "Target": "/app/logs"},
    {"Source": "records", "Target": "/var/lib/web"}
  ]
}
```

```sh
dotnet Tedd.DotNetMesh.dll publish cluster.json web.json ./web-publish
dotnet Tedd.DotNetMesh.dll apply cluster.json web.json
dotnet Tedd.DotNetMesh.dll worker cluster.json
dotnet Tedd.DotNetMesh.dll status cluster.json
dotnet Tedd.DotNetMesh.dll delete cluster.json web
```

Run the worker under the host's service manager. SIGTERM triggers drain and teardown. Scale by updating Replicas and applying the deployment. For a rollback, apply a document naming a retained snapshot and installed image. Status exposes desired deployment, assigned node, generation and worker readiness. A replica is ready when its worker's Ready entry matches the assigned generation and its heartbeat is current.

### Generic Host services

Publish an executable Worker Service or ASP.NET Core application and set `EntryPoint` to its DLL. For a class library containing `IHostedService`, `IHostedLifecycleService` or `BackgroundService` implementations, publish the supplied runner at the root of the release and the workload with its dependencies in a subdirectory.

```sh
dotnet publish src/Tedd.DotNetMesh.ServiceHost -c Release -o ./release
dotnet publish ./Billing.Workers/Billing.Workers.csproj -c Release -o ./release/services
```

Set `EntryPoint` to `Tedd.DotNetMesh.ServiceHost.dll` and select an image containing the .NET 11 ASP.NET Core shared framework. Place the following section in the release root's `appsettings.json`, substituting the assembly path and fully qualified type names. The runner registers service types with the Generic Host in declaration order. Constructors receive dependencies through the host's service provider. The host supplies configuration, logging, application lifetime and an HTTP client factory.

```json
{
  "ServiceHost": {
    "Assembly": "services/Billing.Workers.dll",
    "Types": ["Billing.InvoiceWorker"]
  }
}
```

To supply application-specific dependencies, reference the runner project and implement `Tedd.DotNetMesh.ServiceHost.IServiceModule` on a public class with a parameterless constructor. List its type in `Types`. Its `Configure(IHostApplicationBuilder)` method runs before the host is built and can register dependencies and hosted services. Keep the workload's published `.deps.json`, dependencies and native assets beside its DLL. Socket and HTTP traffic uses the pod network namespace. Mesh-enabled services can register a named HTTP client with the handler returned by `LocalMeshClient.CreateHttpHandler()`.

### GitHub release upgrades

```sh
dotnet Tedd.DotNetMesh.dll github-latest cluster.json web.json OWNER/REPO web-linux.zip
```

Upload a ZIP of the **contents** of the publish directory with the entry DLL at the archive root. Omit the asset name when the latest published release has exactly one uploaded ZIP. For a private repository, set `GITHUB_TOKEN` on the CLI host to a token with repository contents read access. [GitHub release assets](https://docs.github.com/en/rest/releases/assets#get-a-release-asset)

The ZIP can contain up to 16,384 entries and expand to 1 GiB, with a maximum download of 256 MiB. The command checks the SHA-256 digest when GitHub supplies one, ZIP entry integrity, and the deployment's entry point and mount points. It publishes the validated tree before changing the desired deployment and prints the snapshot name for rollback. The command derives the snapshot name from the content, deployment and application mount points. The manifest's `Snapshot` field is optional for this command. Workers reconcile the update through the same rolling procedure as `apply`. Use `status` to check replica readiness.

### Host SMB shares

Each worker can serve its resident pods' persistent data over one encrypted SMB 3.1.1 TCP listener. Put the following `Smb` object inside `Worker` in the cluster configuration. Password files live on the worker host. Give the worker process read access to them and access to the FUSE mounts owned by UID/GID 1654.

```json
{
  "Smb": {
    "ListenAddress": "10.20.0.11",
    "Port": 445,
    "PasswordFiles": {
      "auditor": "/etc/dotnetmesh/smb/auditor",
      "operator": "/etc/dotnetmesh/smb/operator"
    },
    "Shares": {
      "web-0": { "Readers": ["auditor"], "Writers": ["operator"] }
    }
  }
}
```

Connect to `\\\\10.20.0.11\\web-0` with a configured account. The share root is the active ordinal's `/data`, including its persistent directory mappings. Readers can list and read files. Writers can also create, update and delete them. An exact slot ACL overrides the optional `*` default. With neither entry the pod has no share. To exclude one slot from a default grant, give it empty reader and writer lists.

The worker loads passwords and ACLs at startup and can export up to 256 pod shares. A pod replacement withdraws its share and closes its open file handles before unmounting storage. Reconnect to the worker hosting the replacement. Configure the same accounts and ACLs on eligible workers if access must follow a relocating pod. The FUSE backend assigns timestamps and file attributes. Clients that check free space before copying may reject a transfer because the mount does not report available capacity.

### Service gateway

Add the following Gateway property to a cluster configuration with read access to service state, then run dotnet Tedd.DotNetMesh.dll gateway gateway.json. The configured replica addresses must match their HostMesh grants.

```json
{
  "Gateway": {
    "Identity": {"Socket": "/run/hostmesh/edge.sock", "KeyFile": "/etc/dotnetmesh/edge.key"},
    "Routes": [{"ListenPort": 443, "Deployment": "web", "TargetPort": 8443}]
  }
}
```

## Verification

Run `dotnet test` from the repository. The portable suite exercises the service host, SMB accounts and share revocation, GitHub upgrades with a simulated HTTP endpoint, and scheduler handoffs on disposable TDFS clusters.

The Linux filesystem tests mount TDFS through the native bridge and check file operations and encrypted SMB writes across a generation change. Set `MESH_TEST_DIRECTORY` to a short, private Linux path and put `libdotnetmesh_fuse.so` on the loader path. To run the container and failover cases too, set `POD_TEST_SOCKET` and `POD_TEST_IMAGE` to a disposable engine and immutable runtime and `MESH_TEST_FIXTURE` to the published `tests/MeshFixture` directory. The QUIC transport needs `libmsquic.so` on the native loader path. If the package supplies only `libmsquic.so.2`, provide an unversioned symlink to it there. These tests start real workers and containers, upgrade a running service, and kill a worker during service.

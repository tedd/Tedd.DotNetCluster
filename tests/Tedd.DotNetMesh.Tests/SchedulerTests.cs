using Tedd.DotNetMesh;

namespace Tedd.DotNetMesh.Tests;

public class SchedulerTests
{
    static readonly DateTimeOffset Now = DateTimeOffset.UnixEpoch;
    internal static Deployment App => new("service", "r1", "sha256:" + new string('a', 64), "Service.dll", Replicas: 3);
    static ClusterState Start(params (string Id, string Zone)[] nodes)
    {
        var state = new ClusterState();
        state.Deployments["service"] = App;
        foreach (var n in nodes)
            state.Nodes[n.Id] = new(n.Id, n.Zone, Now, 4096, 16000, false, [], new());
        return state;
    }
    static void MarkReady(ClusterState s)
    {
        foreach (var (name, p) in s.Slots) s.Nodes[p.Node].Ready[name] = p.Generation;
    }

    [Fact]
    public void SpreadsAcrossZonesAndNodesAndPreservesHealthyAllocations()
    {
        var s = Start(("a", "x"), ("b", "x"), ("c", "y"));
        Scheduler.Reconcile(s, Now);
        Assert.Equal(3, s.Slots.Values.Select(p => p.Node).Distinct().Count());
        var before = s.Slots.ToArray();
        MarkReady(s);
        Scheduler.Reconcile(s, Now);
        Assert.Equal(before, s.Slots.ToArray());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void GracefulDrainAndSilentExpiryRescheduleOnlyAffectedSlots(bool drain)
    {
        var s = Start(("a", "x"), ("b", "y"), ("c", "z"));
        Scheduler.Reconcile(s, Now);
        var before = s.Slots.ToDictionary();
        s.Nodes["a"] = s.Nodes["a"] with { Drain = drain, Seen = drain ? Now : Now - Scheduler.FailureTimeout };
        Scheduler.Reconcile(s, Now);
        Assert.All(s.Slots.Values, p => Assert.NotEqual("a", p.Node));
        Assert.Equal(3, s.Slots.Count);
        foreach (var p in before.Where(p => p.Value.Node != "a")) Assert.Equal(p.Value, s.Slots[p.Key]);
        Assert.Equal(drain, s.Nodes.ContainsKey("a"));
        s.Nodes["a"] = new("a", "x", Now, 4096, 16000, false, [], new());
        s.Deployments["service"] = App with { Replicas = 4 };
        Scheduler.Reconcile(s, Now);
        Assert.Contains(s.Slots.Values, p => p.Node == "a");
    }

    [Theory]
    [InlineData(255, 3000, 0)]
    [InlineData(5000, 999, 0)]
    [InlineData(512, 2000, 2)]
    [InlineData(1024, 5000, 3)]
    public void EnforcesResourceRequests(int mem, int cpu, int expected)
    {
        var s = Start(("a", "x"));
        s.Nodes["a"] = s.Nodes["a"] with { MemoryMiB = mem, MilliCpu = cpu };
        Scheduler.Reconcile(s, Now);
        Assert.Equal(expected, s.Slots.Count);
    }

    [Fact]
    public void LosingAnEngineOrInstalledImageReschedulesAndRepairsZoneSpread()
    {
        var s = Start(("a", "x"));
        Scheduler.Reconcile(s, Now);
        MarkReady(s);
        s.Nodes["b"] = new("b", "y", Now, 4096, 4000, false, [], new(), [App.Image]);
        Scheduler.Reconcile(s, Now);
        Assert.Equal(1, s.Slots.Values.Count(p => p.Node == "b"));
        s.Nodes["a"] = s.Nodes["a"] with { Images = [] };
        Scheduler.Reconcile(s, Now);
        Assert.Equal(3, s.Slots.Values.Count(p => p.Node == "b"));
    }

    [Theory]
    [InlineData("ready", 1)]
    [InlineData("stale", 0)]
    [InlineData("drained", 0)]
    [InlineData("changed", 0)]
    [InlineData("scaled", 0)]
    public void ServiceResolutionOnlyPublishesRoutableReadyGenerations(string condition, int expected)
    {
        var s = Start(("a", "x"));
        s.Deployments["service"] = App with { Mesh = true, Replicas = 1 };
        s.Nodes["a"] = s.Nodes["a"] with { Bindings = ["service-0"], Addresses = new() { ["service-0"] = "10.1.1.4" } };
        Scheduler.Reconcile(s, Now);
        MarkReady(s);
        if (condition == "stale") s.Nodes["a"] = s.Nodes["a"] with { Seen = Now - Scheduler.FailureTimeout };
        if (condition == "drained") s.Nodes["a"] = s.Nodes["a"] with { Drain = true };
        if (condition == "changed") s.Nodes["a"].Ready["service-0"] = "previous";
        if (condition == "scaled") s.Deployments["service"] = App with { Replicas = 0 };
        Assert.Equal(expected, ServiceGateway.Resolve(s, new(81, "service", 8080), Now).Length);
    }

    [Fact]
    public void MeshPodsRequirePerSlotBindings()
    {
        var s = Start(("a", "x"), ("b", "y"));
        s.Deployments["service"] = App with { Mesh = true };
        s.Nodes["b"] = s.Nodes["b"] with { Bindings = ["service-2"] };
        Scheduler.Reconcile(s, Now);
        Assert.Equal("b", Assert.Single(s.Slots).Value.Node);
        s.Nodes["b"] = s.Nodes["b"] with { Bindings = [] };
        Scheduler.Reconcile(s, Now);
        Assert.Empty(s.Slots);
    }

    [Fact]
    public void ALocalProvisioningFailureMovesOnlyTheAffectedReplica()
    {
        var s = Start(("a", "x"), ("b", "y"));
        Scheduler.Reconcile(s, Now);
        var failed = s.Slots["service-0"];
        var retained = s.Slots["service-1"];
        s.Nodes[failed.Node] = s.Nodes[failed.Node] with { FailedSlots = ["service-0"] };
        Scheduler.Reconcile(s, Now);
        Assert.NotEqual(failed.Node, s.Slots["service-0"].Node);
        Assert.Equal(retained, s.Slots["service-1"]);
    }

    [Fact]
    public void ReadinessGatesRollingReplacementAndOperatorCanRollBack()
    {
        var s = Start(("a", "x"), ("b", "y"));
        Scheduler.Reconcile(s, Now);
        MarkReady(s);
        s.Deployments["service"] = App with { Snapshot = "r2" };
        Scheduler.Reconcile(s, Now);
        var changed = Assert.Single(s.Slots.Values, p => p.Release.Snapshot == "r2");
        Scheduler.Reconcile(s, Now);
        Assert.Single(s.Slots.Values, p => p.Release.Snapshot == "r2");
        s.Deployments["service"] = App;
        Scheduler.Reconcile(s, Now);
        Assert.All(s.Slots.Values, p => Assert.Equal("r1", p.Release.Snapshot));
        Assert.DoesNotContain(s.Slots.Values, p => p.Generation == changed.Generation);
        MarkReady(s);
        s.Deployments["service"] = App with { Snapshot = "r3" };
        for (int n = 1; n <= 3; n++)
        {
            Scheduler.Reconcile(s, Now);
            Assert.Equal(n, s.Slots.Values.Count(p => p.Release.Snapshot == "r3"));
            MarkReady(s);
        }
    }

    [Fact]
    public void ScalingDoesNotRestartRetainedSlotsAndDeletionClearsAssignments()
    {
        var s = Start(("a", "x"));
        Scheduler.Reconcile(s, Now);
        var keep = s.Slots["service-0"];
        s.Deployments["service"] = App with { Replicas = 1 };
        Scheduler.Reconcile(s, Now);
        Assert.Equal(keep, Assert.Single(s.Slots).Value);
        s.Deployments.Clear();
        Scheduler.Reconcile(s, Now);
        Assert.Empty(s.Slots);
    }

    [Theory]
    [InlineData("/../x")]
    [InlineData("/a//b")]
    [InlineData("a")]
    [InlineData("/a:b")]
    [InlineData("/x/")]
    [InlineData("/a\\b")]
    public void RefusesPathEscapesAndAliases(string path) => Assert.Throws<ArgumentException>(() => VolumeMount.ScopedPath("/data", path));

    [Theory]
    [InlineData("/", "/", "/")]
    [InlineData("/data/a", "/", "/data/a")]
    [InlineData("/data/a", "/foo/bar", "/data/a/foo/bar")]
    public void ConfinesPathToMount(string root, string path, string expected) => Assert.Equal(expected, VolumeMount.ScopedPath(root, path));

    [Theory]
    [InlineData(-1)]
    [InlineData(129)]
    public void InvalidReplicaCount(int count) => Assert.Throws<ArgumentException>(() => (App with { Replicas = count }).Validate());

    [Theory]
    [InlineData(62)]
    [InlineData(907)]
    [InlineData(1234)]
    [InlineData(4217)]
    [InlineData(11338)]
    public void ChurnMaintainsUniqueCapacityBoundedAssignments(int seed)
    {
        var random = new Random(seed);
        var s = Start(("a", "x"), ("b", "x"), ("c", "y"), ("d", "z"));
        for (int t = 0; t < 300; t++)
        {
            var now = Now.AddSeconds(t);
            var id = ((char)('a' + random.Next(4))).ToString();
            s.Nodes[id] = new(id, id, now, 1024, 3200, random.Next(6) == 0, [], new());
            s.Deployments["service"] = App with { Replicas = random.Next(11) };
            Scheduler.Reconcile(s, now);
            foreach (var group in s.Slots.Values.GroupBy(p => p.Node))
            {
                Assert.False(s.Nodes[group.Key].Drain);
                Assert.True(group.Sum(p => p.Release.MilliCpu) <= s.Nodes[group.Key].MilliCpu);
                Assert.True(group.Sum(p => p.Release.MemoryMiB) <= s.Nodes[group.Key].MemoryMiB);
            }
            Assert.All(s.Slots, p => Assert.Equal(p.Key, p.Value.Release.Slot(p.Value.Ordinal)));
            MarkReady(s);
        }
    }
}

package main

import "testing"

func TestParseUsageSumsClusterSamples(t *testing.T) {
	stats := "" +
		"gms-stress-n1-1 10.0% 40MiB / 8GiB\n" +
		"gms-stress-n2-1 2.0% 20MiB / 8GiB\n" +
		"gms-stress-n1-1 30.0% 50MiB / 8GiB\n" +
		"gms-stress-n2-1 4.0% 22MiB / 8GiB\n"
	disk := "" +
		"gms-stress-n1-1 1000\n" +
		"gms-stress-n2-1 3000\n"
	got := parseUsage(stats, disk)
	if got.Samples != 2 {
		t.Fatalf("samples %d", got.Samples)
	}
	if got.CPUAvg != 23 || got.CPUPeak != 34 {
		t.Fatalf("cpu avg %.1f peak %.1f", got.CPUAvg, got.CPUPeak)
	}
	if got.Disk != 4000 {
		t.Fatalf("disk %d", got.Disk)
	}
	if len(got.Nodes) != 2 {
		t.Fatalf("nodes %d", len(got.Nodes))
	}
	if shortContainer(got.Nodes[0].Name) != "n1" {
		t.Fatalf("node name %s", got.Nodes[0].Name)
	}
}

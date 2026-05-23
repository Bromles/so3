#!/usr/bin/env python3
"""3-node set test with partition nemesis.

Tests that elements added via successful add operations are never lost,
even during network partitions. The set workload checks eventual inclusion
(all completed adds must appear in subsequent reads), which is weaker than
linearizability and matches the consistency guarantees of quorum reads.
"""

from run import main

if __name__ == "__main__":
    raise SystemExit(
        main([
            "--workload", "g-set",
            "--node-count", "3",
            "--time-limit", "30",
            "--rate", "10",
            "--nemesis", "partition",
            "--nemesis-interval", "5",
        ])
    )

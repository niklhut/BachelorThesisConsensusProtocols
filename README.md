# Bachelor Thesis

This repository contains the source code for my Bachelor's thesis with the title: Performance Evaluation of Consensus Algorithms Across Programming Languages

## Abstract

To keep a consistent state across unreliable networks, distributed systems rely on consensus algorithms such as Raft. While its correctness properties are well established, the influence of programming language design and concurrency models on performance remain open.

This thesis investigates how different runtime architectures and concurrency abstractions affect the performance of Raft implementations. For comparison, implementations were developed in Go, Kotlin and Swift, showing distinct concurrency paradigms by using goroutines and channels, coroutines on the JVM and structured concurrency with actors. The implementations were tested with varying workloads on a server cluster and evaluated for throughput, latency and resource utilization.

The test results show that Kotlin consistently had the best performance, surpassing Go in throughput and latency, contrary to initial expectations that Go's lightweight concurrency model and native runtime would lead. Swift's actor-based design, while offering stronger compile-time safety, showed significantly lower performance, especially under heavy load. Across all implementations the Runtime and Concurrency layer was the dominant performance factor under high load, while the Network layer had the most impact for large messages and Disk I/O revealed limited impact thanks to asynchronous snapshotting.

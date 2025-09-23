# Octar 🐙

An actor model implementation for orchestrating stateful, long-running processes on platforms like [Temporal](https://docs.temporal.io/cloud/limits#workflow-execution-event-history-limits).

### The Problem
Durable execution engines have a finite workflow history size. This presents a challenge for "unbounded" processes (e.g., a user session, a subscription) that may need to run for months or years.

### The Solution
Octar models the process as a system of actors. This system is discrete and marshallable, meaning its entire state can be snapshotted at any point.

When a workflow's history approaches its limit, your application can:
1. Serialize the state of the entire actor system.
2. Pass this state to a new workflow instance using the continue-as-new pattern.
3. Rehydrate the system in the new workflow, allowing the process to continue seamlessly.

This approach provides a deterministic, event-driven model for processes of effectively infinite duration, while still leveraging the fault-tolerance of the underlying platform.

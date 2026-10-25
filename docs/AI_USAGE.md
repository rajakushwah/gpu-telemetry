# How I used AI

I built this project with the **Cursor AI agent**. I owned the design, the decisions and the
review. The AI wrote most of the code, the tests and the build setup, under
explicit constraints and with a clear definition of done for every task.

**Models:** I used **Claude Opus 5.5** for design and code, and **GPT-5.6**
as an independent reviewer of the design. A second model doesn't share the
first one's blind spots, and it found two real concurrency bugs (section 5).


**My rule: nothing went in without my review.** After every step I read the
diff, ran it myself, and questioned anything I didn't fully understand before
moving on. Each section below has a **My review** note saying what I checked and
what I pushed back on.

---

## Workflow at a glance

| Phase | Me (manual) | AI (accelerated) |
|---|---|---|
| Understand the problem | Read the brief, set the hard constraints | Summarised the requirements, analysed the CSV, listed design options |
| Design | Chose the architecture and trade-offs ([HLD.md](HLD.md)) | Drafted design notes for the options I picked |
| Code | Reviewed every package, steered the structure, rejected over-engineering | Wrote the services, the message queue and the DB layer |
| Tests | Defined what must be proven, read and ran every test | Wrote unit, system and smoke tests |
| Build and deploy | Ran everything, reported failures | Dockerfile, Compose, Makefile, Helm; fixed bugs |
| Review and hardening | Asked for targeted review passes, verified the fixes | Found and fixed bugs, measured performance; GPT-5.6 cross-reviewed the design |
| Docs | Edited for accuracy and tone | Drafted the README, HLD and structure docs |

---

## 1. How was the project / repo bootstrapped?

The folder only had the task PDF and the sample CSV, with no code and no git
history. Before any code, I asked the AI to help me understand the problem and
surface the design decisions:

```text
Context: Interview take-home, "Elastic GPU Telemetry Pipeline with a Message
Queue". The brief (PDF) and sample data (CSV) are in the repo.

Task: Do NOT write code yet. Read the brief and inspect the CSV, then give me:
1. A requirements checklist (functional + non-functional + deliverables).
2. The open design questions, each with 2-3 options and their trade-offs.
3. A proposed high-level architecture.

Hard constraints:
- Build our own message queue. Kafka, RabbitMQ, ZeroMQ etc. are not allowed.
- The telemetry timestamp is the processing time (time.Now()), not the CSV time.
- Streamers and collectors scale independently in both directions
  (1 streamer -> N collectors, N streamers -> 1 collector), up to 10 each.
- Database is our choice, but it needs a solid justification.
```

**What the AI did:** it inspected the CSV and found the detail that shaped the
design: `gpu_id` is only `0–7` and repeats on every host, so the **GPU UUID** is
the real identifier. It then laid out the options.

**What I decided** (the AI only gave options):

- **Shard** CSV rows across streamers instead of having each one replicate all
  of them.
- **A single broker backed by a write-ahead log**, with replication left as
  future work.
- **TimescaleDB** for the database.
- **Design document first**, then code.

Then the AI set up the Go module and the folder layout (`cmd/` for entry
points, `internal/` for logic).

**My review:** I checked its requirements checklist line by line against the
PDF before agreeing to anything, and asked it to confirm it understood the
scaling and timestamp rules. I verified the `gpu_id` finding in the CSV myself
before accepting UUID as the key.

---

## 2. How was the code bootstrapped?

```text
Role: Act as a senior staff engineer.

Task: Implement the full project against the agreed design.

Quality bar:
- Secure by default: validate all input, no hard-coded secrets, non-root containers.
- Clean design: small interfaces, single responsibility, easy to extend.
- Scalable: stateless workers, partitioned queue, horizontal scaling.
- Minimal code: no speculative abstractions. One short comment per section,
  explaining "why", not "what".

User experience: someone with only Docker installed should be able to run the
whole stack with one command.

Definition of done: go build, go vet and go test pass, and the stack starts and
serves real data end to end.
```

**What the AI did:** it built the message queue first (write-ahead log,
partitions, consumer groups, fenced commits, backpressure), then the streamer,
collector, store and REST API, building and testing as it went.

**Where I steered it, or it got things wrong:**

- It planned **gRPC**; I switched the transport to **plain HTTP/JSON** so it's
  easy to debug with curl and needs no code generation.
- `go vet` caught a possible **context leak** in the consumer; it was fixed by
  extracting a small `workerSet` helper.
- Its first database code **didn't compile** (it called functions that don't
  exist), so it was rewritten more simply.

**My review:** I went through each package as it was written: the queue
(partitioning, commits, backpressure), then the streamer, collector, store and
API. I checked that the code matched the design rather than inventing its own.
I traced where the timestamp is set, to confirm it really is `time.Now()` at
processing time and that the CSV time is ignored. I tested every endpoint in
Swagger myself and questioned the API design, for example what `cursor`
means, and which ID a user should pass, given that `gpu_id` repeats across hosts.

---

## 3. How were the unit tests developed?

```text
Task: Prove every requirement in the brief, one by one.

1. Unit tests for the invariants that matter, not for line coverage:
   - Queue: crash recovery (torn tail truncated), backpressure instead of
     eviction, fenced commits after a rebalance, round-robin assignment.
   - CSV parsing (columns found by header, bad rows skipped), store
     dedup and ordering, API validation and error codes.
2. One system test that runs the whole pipeline in-process on the real CSV
   with 2 streamers and 2 collectors, and checks every GPU arrives.
3. A smoke script against the running Docker stack.

Report pass/fail per requirement. Run everything with the race detector.
```

Later, after the code review (section 5), I asked for tests that pin down each
bug that was fixed:

```text
For every bug you fixed, add the smallest test that would have caught it.
Then remove any test that duplicates another or only checks trivial code;
one focused test per behaviour.
```

**Result:** unit tests in every package, a system test in `test/e2e/`, and
`scripts/smoke_test.sh` with 13 live checks.

**A real catch:** the coverage number kept jumping between runs (56% to 80%)
without any code changing. The cause: on my Mac the Go toolchain doesn't merge
per-package coverage profiles, and stale cached results were being mixed in.
The AI fixed the Makefile to merge duplicate blocks and skip the test cache.
Coverage is now stable at **~76%**.

**My review:** I read the tests to make sure they assert real behaviour, not
just call functions for coverage. I ran them myself, including with the race
detector, and questioned the coverage number when it didn't add up. That's
how the reporting bug was found. When the AI added too many tests, I asked it to
cut them back to one focused test per behaviour.

---

## 4. How was the build env bootstrapped?

```text
Task: Package and deploy the stack.

- One multi-stage Dockerfile producing a distroless, non-root image with all
  four binaries.
- docker-compose for local runs; the DB password is generated into a git-ignored
  .env, never committed.
- Helm chart for Kubernetes: broker and DB as StatefulSets with volumes, workers
  and API as Deployments, generated Secret, NetworkPolicies.
- Makefile targets: demo, up, scale, down, test, coverage, openapi.

Definition of done: on a clean machine with only Docker, `make demo` builds,
starts and verifies the stack.
```

**Real bugs that only showed up when I ran it:**

- `make up` failed with `docker: No such file or directory`. Docker Desktop's
  CLI wasn't on my PATH, so the Makefile now finds it on its own.
- The image used **Go 1.24** while `go.mod` needs **1.25**, so the build failed;
  the base image was bumped.
- The container couldn't read the CSV (the file was mode 600 and the directory
  wasn't traversable for the non-root user); it was fixed in the Dockerfile.

**My review:** I ran every build myself and pasted back the real errors
instead of accepting "it should work". I checked that the image runs as
non-root, that no password is in the repo or the image, and that `.env` is
git-ignored. I asked for the run steps to be cut to the minimum, so that anyone
with only Docker can start it with `make demo` or `./run.sh`. I also went through
the deliverables list from the brief and checked each one off against the repo.

---

## 5. Review and hardening passes (manual direction, AI execution)

Once it worked, I ran targeted review passes. Each prompt had one focus:

**Code quality review**

```text
Review the codebase as a senior staff engineer: extensibility, scalability,
minimal and accurate comments. List concrete defects with file and line, fix
them, and prove each fix with a test. Don't refactor for taste.
```

This caught real bugs. Requests that could never succeed (HTTP 400) were
retried forever. An out-of-range commit returned the wrong error, which made a
collector drop its partition.

**Concurrency and error handling**

```text
Audit goroutine lifecycles, channels, WaitGroups, locking and error paths.
Use a worker pool or other concurrency only where it measurably helps;
explain where you deliberately didn't add it.
```

This caught a heartbeat goroutine that wasn't waited on before shutdown (it
could re-register a "ghost" streamer), a flush goroutine racing with broker
shutdown, and an offsets file that wasn't fsynced before being renamed.

**Minimalism**

```text
Remove anything not required for this use case: unused abstractions,
duplicate tests, comments that restate the code. Behaviour must not change.
```

**Scale, performance and availability**

```text
Measure the running system (throughput, memory, CPU, storage growth) and
check the design holds at 10 streamers and 10 collectors. Document the
numbers and the single points of failure in the HLD.
```

Measuring showed the database had grown to **55 GB in about 10 hours**, which
would mean about 900 GB at 7-day retention. Enabling TimescaleDB compression
cut storage **74×**, and I raised the Helm volume size to match.

**Cross-model design review (GPT-5.6)**

Everything above was written by Claude, so I had GPT-5.6 review the design
cold, read-only:

```text
Role: Principal distributed-systems engineer doing an independent design review.
Read-only; don't edit files.

Read HLD.md, then spot-check the code it describes (queue, streamer, schema)
to verify each claim is actually true.

Return: a verdict for the stated constraints (custom queue, processing-time
timestamps, up to 10 streamers/collectors); the top findings by severity,
with file references and only issues verified in code; anything in HLD.md that
is inaccurate versus the code; and the decisions you agree with.
```

It found issues the authoring model had missed. Each one was checked against
the code before acting on it:

- **Fixed: fencing race.** A commit's ownership check and its offset write
  used different locks, so a rebalance in between could let a stale collector
  overwrite the new owner's progress. Membership changes and commits now share
  one lock.
- **Fixed: durability ordering.** Offsets were saved after the log fsync, so a
  saved offset could point past records not yet on disk. Offsets are now
  snapshotted before the fsync.
- **Documented: streamer rescale window.** For up to 2 s after a rescale,
  streamers can briefly disagree on shard ownership. This is acceptable for
  telemetry; a strict fix (an epoch-fenced handoff) is noted in the HLD.
- **Documented: abandoned consumer groups.** A group that is created and never
  used again would hold back backpressure. This doesn't apply with our single
  group, so it's noted in the HLD.
- **Corrected in HLD.md:** two claims that were too strong ("exactly one
  owner" now says "in steady state", and "secure by default" now notes that TLS
  is future work). A third, "a stale collector can never overwrite progress",
  became true with the fencing fix.

**My review:** I started each pass myself because I wanted a specific
property checked; the AI didn't volunteer these. For every fix I asked why it
was needed, and checked that it came with a test or a live measurement. I
rejected changes that added code without a clear need. I also verified the
scaling claims live myself (for example 6 streamers with 5 collectors) instead
of trusting the description.

---

## 6. Documentation

The AI drafted the README, [HLD.md](HLD.md) and
[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md). I had it check every number and
claim against the running system (for example, the scaling table comes from a
live run with 6 streamers and 5 collectors) and rewrite the docs to be short and
plain.

**My review:** I read every doc against the brief's README checklist and the
actual code. I asked for missing pieces (design-flow diagrams for the queue, the
scaling section, the HLD) and removed anything that overstated what the system
does.

---

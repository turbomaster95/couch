# Couch Pipeline Configuration

Couch supports repository-defined pipelines through TOML files stored in a
`.couch/` directory at the root of a repository.

Each TOML file defines one stage of the pipeline. Files are sorted by filename and executed in that order.

## Directory Layout

A typical repository might contain::

```
.couch/
    00-build.toml
    01-test.toml
    02-package.toml
```

The filename prefix determines the execution order. Use zero-padded numbers
to keep stages in the intended order.

For example, `00-build.toml` executes before `01-test.toml`.

Pipeline files must be valid TOML. Couch should validate all pipeline files
before executing any stage so that a configuration error does not leave a
partially executed pipeline.

If the repository does not contain a `.couch/` directory, Couch falls back
to the legacy job command supplied when the job was created.

## Basic Configuration

A pipeline file contains a `[stage]` table and one or more `[[steps]]`
tables.

Example::

```
[stage]
name = "build"
timeout = 300

[[steps]]
name = "configure"
run = "cmake -S . -B build"

[[steps]]
name = "compile"
run = "cmake --build build --parallel 2"
```

The `[stage]` table describes the stage. Each `[[steps]]` table describes
a command executed as part of that stage.

## Stage Options

The following options can be specified in the `[stage]` table.

`name`
A descriptive name for the stage.

```
Example::

    name = "build"
```

`timeout`
Maximum stage execution time in seconds.

```
Example::

    timeout = 300
```

`needs`
An array of stage names that must have completed successfully before this
stage can run.

```
Example::

    needs = ["build", "test"]

Dependencies refer to the ``name`` values of other stages, not their
filenames. A dependency must refer to a stage defined in the pipeline.
```

`when`
Controls when the stage is eligible to run.

```
Supported values:

``"success"``
    Run only while the pipeline has not encountered a failure.

``"always"``
    Allow the stage to run even if an earlier stage failed. This is useful
    for cleanup and diagnostic collection.

Example::

    when = "always"

A stage's dependencies must still be satisfied. ``when = "always"`` does
not override an unmet dependency.
```

`allow_failure`
Indicates whether a stage failure should be treated as non-fatal.

```
Example::

    allow_failure = true

Use this for optional checks that should not determine the overall
pipeline result. Verify this behavior against the installed runner
version, as failure handling may vary by implementation.
```

`retries`
Number of additional attempts permitted after a stage fails.

```
Example::

    retries = 2

A value of ``2`` permits up to three total attempts: the initial attempt
and two retries. Retry behavior depends on support in the installed
runner version.
```

## Steps

Steps are declared using TOML array-of-table syntax. They execute in the
order in which they appear in the file.

Each step supports the following options where implemented.

`name`
A descriptive name for the step.

`run`
The shell command to execute.

`timeout`
Maximum execution time for the step, in seconds, when supported by the
runner. If a step timeout is not specified, the stage timeout applies
where supported.

Example::

```
[[steps]]
name = "configure"
run = "cmake -S . -B build"

[[steps]]
name = "compile"
run = "cmake --build build --parallel 2"
timeout = 240
```

Commands run in the checked-out repository workspace unless a different
working directory is configured and supported by the runner.

A failed command normally causes its stage to fail. Subsequent steps in the
same stage may not execute after a failure.

## Environment Variables

Environment variables can be used to configure build tools and commands.
The exact TOML option for defining stage- or step-specific environment
variables must match the runner version in use.

Commands can reference environment variables using the shell syntax
supported by the runner's execution shell. For example::

```
[[steps]]
name = "build"
run = "cmake -S . -B build -DCMAKE_BUILD_TYPE=$BUILD_TYPE"
```

Do not place secrets directly in committed pipeline files. Use an appropriate
secret-injection mechanism if one is provided by your Couch deployment.

## Working Directory

A stage or step may support a working-directory option, depending on the
runner implementation.

Paths should normally be relative to the checked-out repository. Absolute
paths can expose files outside the workspace and should not be used for
untrusted projects.

## Artifacts

Artifacts are files collected from a stage and uploaded to Couch for later
inspection or download.

Declare artifact paths in an `[artifacts]` table::

```
[artifacts]
paths = ["build/my-program", "build/Testing/Temporary/LastTest.log"]
```

`paths`
An array of files or paths to collect.

Artifact paths should be relative to the repository workspace. Ensure that
the requested files exist before artifact collection.

Artifacts are intended to remain useful even when a stage fails. For example,
a test stage can upload logs that explain why a test suite failed.

Artifact uploads use Couch's existing artifact upload protocol. Large
artifacts may be transferred in chunks.

## Dependencies and Execution Order

Couch uses lexicographic filename order to determine the order in which
pipeline stages are considered. The `needs` option expresses dependencies
between stages.

For example, the following configuration describes a build stage followed
by a test stage.

`00-build.toml`::

```
[stage]
name = "build"

[[steps]]
name = "compile"
run = "make"
```

`01-test.toml`::

```
[stage]
name = "test"
needs = ["build"]

[[steps]]
name = "tests"
run = "make test"
```

The test stage depends on the successful completion of the build stage.

A dependency does not change the lexicographic ordering of files. Arrange
filenames so that prerequisite stages appear before the stages that depend
on them.

## Conditions and Cleanup

Use `when = "always"` for tasks that should be considered after a previous
stage fails, such as collecting diagnostic information.

Example::

```
[stage]
name = "diagnostics"
when = "always"

[[steps]]
name = "collect-logs"
run = "find build -type f -name '*.log' -print"
```

The stage can still be skipped if its declared dependencies are not
satisfied.

## Runner Labels

Pipeline configurations may specify runner labels to restrict execution to
suitable runners, depending on the installed runner version.

Example of a conceptual label requirement::

```
[stage]
name = "linux-build"
labels = ["linux", "x86_64"]
```

Only use label options that are recognized by your version of Couch. Label
matching and the set of available labels depend on runner registration and
implementation.

## Caching

Caching can reduce build times by preserving dependencies or other reusable
files between jobs.

A conceptual cache configuration might look like::

```
[cache]
paths = ["build/deps", ".cache"]
```

Persistent cache support must be available in the installed runner before
this configuration can be relied upon. A declared cache path does not by
itself guarantee that files are saved, restored, or shared between jobs.

## Complete Example

The following pipeline builds a project, runs its tests, and packages the
result.

`.couch/00-build.toml`::

```
[stage]
name = "build"
timeout = 300
retries = 0

[[steps]]
name = "configure"
run = "cmake -S . -B build -DCMAKE_BUILD_TYPE=Release"

[[steps]]
name = "compile"
run = "cmake --build build --parallel 2"
timeout = 240

[artifacts]
paths = ["build/my-program"]
```

`.couch/01-test.toml`::

```
[stage]
name = "test"
needs = ["build"]
timeout = 120

[[steps]]
name = "run-tests"
run = "ctest --test-dir build --output-on-failure"

[artifacts]
paths = ["build/Testing/Temporary/LastTest.log"]
```

`.couch/02-package.toml`::

```
[stage]
name = "package"
needs = ["test"]
when = "success"
allow_failure = false

[[steps]]
name = "create-package"
run = "tar -czf my-program.tar.gz build/my-program"

[artifacts]
paths = ["my-program.tar.gz"]
```

## Configuration Validation

Pipeline files should be checked for the following problems before execution:

* Invalid TOML syntax.
* Missing or invalid stage names.
* Missing step commands.
* Unsupported option types or values.
* Dependencies referring to unknown stages.
* Invalid timeout or retry values.
* Invalid artifact path declarations.

Keep configuration files small and use descriptive stage and step names.
This makes job logs easier to read and failed builds easier to diagnose.

## Security

Pipeline commands execute shell commands from the repository. A repository
can therefore execute arbitrary commands with the permissions of the runner
account.

Run workers under restricted operating-system accounts and use container or
other isolation mechanisms when processing repositories you do not trust.
Do not expose host credentials, privileged sockets, or sensitive files to
untrusted jobs.

## Version Compatibility

The options accepted by a pipeline are determined by the installed Couch
runner version. Consult the implementation when relying on optional features
such as environment declarations, working directories, runner labels,
retries, or persistent caching.

Unsupported options should not be assumed to work merely because they are
valid TOML.

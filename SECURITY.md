# Security policy

## Reporting a vulnerability

**Do not open a public issue.** Use GitHub's private reporting:
[**Report a vulnerability**](https://github.com/lolaf-org/ringos/security/advisories/new). It is
visible only to the maintainer until an advisory is published.

Please include the ringos version, the JDK and the JVM flags in use, and enough detail to reproduce
it. Ringos is maintained by one person: expect an acknowledgement within a week, and a fix released
as soon as one exists rather than on a schedule. You will be credited in the advisory unless you ask
not to be.

## Supported versions

Nothing is released yet. Once 0.1.0 is out, fixes go to the latest released version, and while the
version is 0.x that means the latest minor — there are no maintenance branches.

## Scope

Ringos reaches into JDK internals on purpose. `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED`
selects the `Unsafe`-backed implementations, and `ringos-unsafe-operations-api` exposes raw memory
access to the caller that asks for it. That is documented behaviour under flags the application
chooses, not a vulnerability — an application that grants it is deciding to trust its own
dependencies with unchecked memory access.

What is in scope is ringos doing something the caller did not ask for: memory reached outside a
buffer's own allocation, an implementation selected that the running JVM does not actually support,
or an input from a caller's data corrupting the buffer's state.

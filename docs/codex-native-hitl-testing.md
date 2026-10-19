# Native Codex question integration test

`TestNativeAppServerQuestionLifecycle` runs the real `appServerSession` adapter
against an actual Codex 0.158.0 app-server. Its model is a deterministic Responses
server listening on loopback. This validates the transport and lifecycle, not a
live model's decision to ask a question and not Feishu client rendering.

The test uses fresh temporary project and Codex-home directories, requires no
credentials, executes no business command, and sends no Feishu message. Its
temporary configuration enables `features.default_mode_request_user_input`.
That feature is disabled by default in Codex 0.158.0; passing this test does not
mean a production configuration has enabled it.

Run the native test on a host with the audited Codex binary:

```sh
go test -c ./agent/codex -o /tmp/cc-connect-codex-native.test
env -i PATH="/path/to/node/bin:/usr/bin:/bin" LANG=C.UTF-8 \
  CC_CONNECT_CODEX_NATIVE_BIN=/path/to/codex \
  /tmp/cc-connect-codex-native.test \
  -test.run TestNativeAppServerQuestionLifecycle -test.v -test.timeout 90s
```

For a Linux amd64 target, build the test executable with
`GOOS=linux GOARCH=amd64 go test -c ./agent/codex -o /tmp/cc-connect-codex-native.test`
and transfer only that executable. Run under `env -i` as shown above, without
copying an existing Codex configuration, authentication file, or service
environment. Include the Node binary directory in `PATH` when the Codex
launcher is installed through npm.

Both `prod` and `stg` branches check:

- A real `item/tool/requestUserInput` reaches the adapter with its request,
  thread, turn, and question identifiers.
- During the complete 400 ms observation window, no completion or error occurs
  and the model receives no follow-up request.
- The selected answer returns to the original native request. The same thread
  has exactly one completed turn with the original turn identifier.
- A duplicate answer is rejected.
- A second native turn interrupted while waiting invalidates its pending
  question, rejects a stale answer, and does not resume the model with empty
  answers.

Run deterministic adapter regressions, including failed response writes and
concurrent duplicate answers, separately:

```sh
go test -race ./agent/codex \
  -run 'TestAppServerQuestion|TestAppServerApproval|TestAppServerSession_HandleRequestUserInput' \
  -count=1
```

The native test is skipped by ordinary test runs unless
`CC_CONNECT_CODEX_NATIVE_BIN` is set. Feishu callback binding, sender isolation,
stale-card handling, and card delivery are covered by separate platform and core
tests; they are not claimed by this native test.

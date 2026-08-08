# Rust agent behavior

## Concurrency and buffering

Each configured file has one tailer task. Tailers parse complete lines and use `try_send` on a bounded in-memory channel; they never wait behind HTTP delivery. A full channel drops the new event and increments `queue_dropped`. Oversized lines are also bounded and counted separately. One batcher task owns the receiver, so batch assembly and delivery order do not require a mutex.

The batcher flushes when `batch.max_size` is reached, when `batch.flush_interval_ms` expires, or when all tailers close their senders during shutdown. An event ID is created during parsing and the same in-memory event is reused for every HTTP attempt.

## Delivery failures

Every HTTP request has `backend.request_timeout_ms` as a deadline. Network failures, response decoding failures, HTTP 408/429, and 5xx responses are transient. Other 4xx responses are permanent and are not retried. Transient failures use exponential backoff capped by `retry.max_delay_ms`, with symmetric jitter controlled by `retry.jitter_ratio`, and stop after `retry.max_attempts`.

The agent holds failed batches only in memory. If all attempts fail, those events are counted as `delivery_dropped`; there is no local disk spool in this milestone.

## File semantics

The tailer polls at 250 ms, reads only newly appended bytes, and retains an unterminated final line until it receives a newline. On graceful shutdown or before switching to a replacement/truncated stream, that partial line is emitted once.

Truncation is detected when the current file becomes shorter than the consumed offset. Rename-and-create rotation is detected by file identity (device/inode on Unix and creation time on Windows), then reading restarts at byte zero of the new path. The agent intentionally does not keep an open handle to drain lines appended to the renamed file after rotation; those late writes are unsupported. Invalid UTF-8 is preserved as far as the JSON string contract allows by replacing invalid sequences with the Unicode replacement character.

Offsets are not checkpointed. A process restart rereads configured files from byte zero and creates new event IDs, so deployments that require restart-safe collection need a persistent checkpoint/spool layer.

## Shutdown

Ctrl-C and Unix `SIGTERM` stop tailers first. Closing the final sender instructs the batcher to flush its remaining batch. An unexpected tailer, batcher, or observability-server exit initiates the same bounded shutdown and makes the process exit non-zero. The entire sequence is bounded by `shutdown_timeout_ms`; after the deadline, tasks are cancelled and structured logs explicitly warn that in-flight events may have been lost.

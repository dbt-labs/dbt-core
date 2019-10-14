"""The `rpc` package handles most aspects of the actual execution of dbt's RPC
server (except for the server itself and the client tasks, which are defined in
the `task.remote` package).

The general idea from a thread/process management perspective (ignoring the
--single-threaded flag!) is as follows:

- The RPC server runs a web server, in particular `werkzeug`, which manages a
  thread pool.
- When a request comes in, werkzeug spins off a thread to manage the
  request/response portion. dbt itself has basically no control over this
  operation - from our viewpoint request/response cycles are fully
  synchronous.
- synchronous requests are defined as methods in the `TaskManager` and handled
  in the responding thread directly.
- Asynchronous requests (defined in `tasks.remote`) are kicked off wrapped in
  `RequestTaskHandler`s, which manage a new process and a new thread.
    - The process runs the actual dbt request, logging via a message queue
    - eventually just before process exit, the process places an "error" or
      "result" on the queue
    - The thread monitors the queue, taking logs off the queue and adding them
      to the `RequestTaskHandler`'s `logs` attribute.
    - The thread also monitors the `is_alive` state of the process, in case
      it is killed "unexpectedly" (including via `kill`)
    - When the thread sees an error or result come over the queue, it join()s
      the process.
    - When the thread sees that the process has disappeared without placing
      anything on the queue, it checks the queue one last time, and then acts
      as if the queue received an 'Unexpected termination' error
- `kill` commands pointed at an asynchronous task kill the process and allow
  the thread to handle cleanup and management
- When the RPC server receives a shutdown instruction, it:
  - stops responding to requests
  - `kills` all processes (triggering the end of all processes, right!?)
  - exits (all remaining threads should die here!)
"""

# Memory Base Specification

## Overview
A **Memory Base** is a feature in Langflow that automatically captures and retains the message history (outputs) of a flow and periodically flushes them into a **Knowledge Base**. This allows flows to "remember" their past outputs in a vector-searchable format without manual intervention.

## Core Concepts

### 1. Memory Base
A configuration associated with a specific Flow that tracks:
- The associated **Flow**.
- A **Threshold** (number of messages) that triggers the vectorization process.
- The target **Knowledge Base**.
- Deployment metadata (pending messages, last ingestion time, total processed).

### 2. Stride
A **Stride** is defined as the set of messages/outputs generated during a single invocation (run) of the flow.
- Ingestion jobs are triggered based on the total message count exceeding the threshold, but the units of collection are strides.

### 3. Knowledge Base Integration
- Memory Bases use standard Langflow Knowledge Bases (KBs) for storage.
- KBs marked as "Memory Base" are hidden from the **Knowledge Retrieval** component to prevent circularity or clutter but remain visible in the main Knowledge Base listing.

---

## Technical Design

### 1. Database Model Changes

#### `MemoryBase` (New)
Configuration for monitoring a flow.
| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | `UUID` (PK) | Unique identifier. |
| `name` | `String` | User-defined name. |
| `flow_id` | `UUID` (FK) | Flow to monitor. |
| `user_id` | `UUID` (FK) | Owner ID. |
| `threshold` | `Integer` | Per-session message count trigger. |
| `kb_name` | `String` | Target Knowledge Base name. |
| `auto_capture` | `Boolean` | Whether it automatically collects new messages. |
| `created_at` | `DateTime` | Creation timestamp. |

#### `MemoryBaseSession` (New)
Tracks the progress of ingestion for individual sessions (conversations) within a Memory Base.
| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | `UUID` (PK) | Unique identifier. |
| `memory_base_id` | `UUID` (FK) | Target Memory Base. |
| `session_id` | `String` | The conversation/session identifier. |
| `cursor_id` | `UUID` (Nullable) | Last processed message ID for this specific session. |
| `total_processed` | `Integer` | Messages vectorized for this session. |
| `last_sync_at` | `DateTime` | Last successful sync for this session. |

#### `Message` (Extended)
- `run_id`: `UUID | None`. Identifies specific invocation.
- `is_output`: `bool`.

### 2. Knowledge Base Metadata
Modify `embedding_metadata.json` for KBs to include `is_memory_base: true`.

### 3. Workflow Logic

#### A. Automatic Collection & Session Tracking
In the Flow Execution Engine:
1. Identify `MemoryBase` entries for the `flow_id` where `auto_capture == True`.
2. Save current invocation outputs to `Message` table with `run_id`, `is_output = True`, and the current `session_id`.
3. **Session State**: For each active `MemoryBase`, ensure a `MemoryBaseSession` entry exists for the current `(memory_base_id, session_id)`.
4. **Trigger Check**: Count pending messages *for this session*: `SELECT COUNT(*) FROM message WHERE session_id = ? AND id > cursor_id AND is_output = True`.
5. If `Pending Count >= Threshold`, trigger `IngestMemoryTask(memory_base_id, session_id)`.

#### B. IngestMemoryTask (Session-Scoped)
1. **Fetch**: `SELECT * FROM message WHERE session_id = ? AND flow_id = ? AND id > cursor_id AND is_output = True ORDER BY timestamp ASC`.
2. **Ingest**: Use `KBIngestionHelper` to upsert these session-specific messages to the target KB.
3. **Update**: Update `MemoryBaseSession.cursor_id`, `last_sync_at`, and `total_processed` for the specific session.

#### C. Manual Ingestion
An API endpoint `POST /memories/{id}/update` (or `/flush`) that triggers the ingestion sync immediately regardless of the threshold status.

### 4. Visibility Constraints
- **Knowledge Base Listing**: Show all KBs.
- **Knowledge Retrieval Component**: Update logic to filter out KBs where `is_memory_base == true`.

## Reliability & Error Handling

### 1. Atomic Cursor Updates
To prevent data loss or double-ingestion:
- **No Optimistic Commits**: The `MemoryBaseSession.cursor_id` must NEVER be updated before the ingestion job confirms success.
- **Success Confirmation**: The cursor update must be the final step of the `IngestMemoryTask`. It should only occur after the `KBIngestionHelper` confirms that all message chunks have been successfully persisted in the target Knowledge Base (Vector DB).
- **Retry Mechanism**: If a job fails, the `cursor_id` remains unchanged. The next execution (either manual or automatic) will attempt to fetch and ingest from the last known successful point.

### 2. Cleanup & Consistency
- **Ingestion Failures**: If the vectorization job fails midway, any partial data in the Vector DB (Chroma) should ideally be handled by the specialized KB ingestion logic (which often handles idempotency or overwrites based on chunk metadata).
- **Rollback**: Database transactions for the `MemoryBaseSession` updates must only be committed upon task completion.

## Edge Cases & Advanced Reliability

### 1. Resource Cleanup & Cancellation
- **Deletion During Sync**: If a `MemoryBase` is deleted while a synchronization task is active, the system must forcefully cancel the associated background tasks via the `TaskService` before completing the database deletion.
- **Concurrent Task Prevention**: To avoid redundant indexing and race conditions, the system must allow only ONE active `IngestMemoryTask` per `(memory_base_id, session_id)`. If another task is triggered (either automatically or manually via `/update`), the API must return a `409 Conflict` error.

### 2. Synchronization & Threshold Logic
- **Immutable Job Arguments**: When a sync job is queued, all associated parameters (e.g., current `cursor_id`, message range) must be passed as immutable arguments to the task.
- **Threshold (Batch Size) Updates**: If the `threshold` for a `MemoryBase` is updated by the user while messages are pending:
    - The current pending count should only be re-evaluated against the new threshold during the *next* auto-capture trigger.
    - Any already-running ingestion task will ignore the new threshold and proceed with its original scope.

### 3. File System & Vector DB Mismatches
- **External Deletion Recovery**: In cases where the underlying Knowledge Base files are deleted manually from the filesystem (e.g., Chroma directory cleanup):
    - The system should detect the mismatch (metadata shows `total_processed > 0` but the vector store is empty).
    - The UI should surface a "Mismatch Detected" warning and offer a **Regenerate** option.
    - **Regenerate Logic**: This resets the `cursor_id` to `None` for all associated sessions and re-runs the ingestion task from the beginning to fully reconstruct the Knowledge Base.

---

## API Endpoints

### 1. Message Retrieval
- `GET /messages?session_id=<session_id>&memory_base_id=<id>&page=<>&offset`
  - Fetch messages filtered by session or specific Memory Base context.
  - Supports pagination.
  - **Verification**: Needs to extend the monitor message service to resolve `memory_base_id` to its associated `flow_id`.

### 2. Memory Base Management
- `POST /memories`
  - Create a memory base with the schema payload.
- `GET /memories`
  - List all memory bases for the current user.
- `GET /memories/{id}`
  - Retrieve details for a specific memory base configuration.
- `GET /memories/{id}/sessions`
  - Retrieve a list of all sessions (conversations) being tracked by this Memory Base, including per-session sync status and message counts.
- `PATCH /memories/{id}`
  - Update parameters (e.g., toggle `auto_capture`, threshold).
- `DELETE /memories/{id}`
  - Delete a specific memory base.
- `POST /memories/{id}/update` (or `/memories/{id}/flush`)
  - Manually trigger an ingestion/sync job for the specified Memory Base.

---

## Constraints & Guards
- **DB Load**: Use indexed queries for message counting.
- **Task Queue**: Use `TaskService` for background vectorization.
- **Concurrency**: Use `cursor_id` to prevent redundant ingestion.

## UI Requirements (Brief)
- Flow settings "Memory" tab.
- Toggle for "Auto-capture".
- "Sync Now" button and status counters.

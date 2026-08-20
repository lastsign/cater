# kafka_service

Kafka as the main transport for indexing work — instead of Celery. Each pipeline stage
is its own consumer group, and the stages are wired together by topics:

```
index.requests ──► [fetch]  ──► content.fetched ──► [chunk] ──► chunks.ready ──► [embed] ──► index.done
       ▲                │                  │                             │
   FastAPI /            └──────────────────┴─────────────────────────────┴──► index.events (statuses, WS)
   CLI submit                                                                 index.dlq    (failures)

Postgres WAL ──► Debezium ──► cater.public.chunks ──► [cdc] ──► point deletion in Qdrant
```

Where things live: Postgres is the source of truth (`Content`, `ContentText`, `Chunk`),
Qdrant holds the vectors, Kafka carries identifiers only (`doc_id`, `url`). Texts and
vectors are never pushed through the broker.

## Files

| File | Purpose |
|---|---|
| `config.py` | topics, groups, producer/consumer configs, timings (all via env) |
| `schemas.py` | `Envelope[T]` + payloads: `IndexRequest`, `ContentFetched`, `ChunksReady`, `IndexDone`, `StatusEvent`, `StageFailed` |
| `stages.py` | stage handlers — pure functions, know nothing about Kafka, tested directly |
| `worker.py` | the consume → handler → produce → commit loop, retries, DLQ, graceful shutdown |
| `sync_client.py` | confluent-kafka (librdkafka): producer + consumer factory — for the workers |
| `async_client.py` | aiokafka: `producer.submit_index_request()` and the `index.events` → WebSocket bridge — for FastAPI |
| `events.py` | projection of `Envelope[StatusEvent]` into a flat event for WS and for the DB |
| `projector.py` | consumer of `index.events` → the `request` table (snapshots for WS) |
| `admin.py` | explicit topic creation (broker auto-creation is off) |
| `cdc.py` | CDC sink Postgres → Qdrant: removal of orphaned points + a one-off `sweep` |
| `debezium/application.properties` | Debezium Server config (mounted into the container) |
| `__main__.py` | CLI: `topics`, `run`, `submit`, `tail`, `replay`, `run-cdc`, `sweep` |

## Running

```bash
uv sync --group worker --group kafka          # workers (fetch/chunk/embed)
uv sync --group server --group kafka          # FastAPI

docker compose -f src/docker-compose.yml up -d kafka
python -m src.kafka_service topics            # create the topics

# one process per stage (production layout)
python -m src.kafka_service run fetch
python -m src.kafka_service run chunk
python -m src.kafka_service run embed

# or everything in one process — for local debugging
python -m src.kafka_service run fetch chunk embed --ensure-topics

python -m src.kafka_service run-projector      # index.events statuses -> the request table

python -m src.kafka_service submit https://huggingface.co/blog/some-post
python -m src.kafka_service tail index.events
```

Scaling: start N processes of one stage with the same `group.id` — Kafka distributes the
partitions itself. The parallelism ceiling is the partition count
(`KAFKA_TOPIC_PARTITIONS`, 6 by default).

## Wiring into FastAPI

The module patches nothing in `main.py` — here is the minimal glue:

```python
from src.kafka_service.async_client import producer, run_event_pump
from src.kafka_service.projector import run_status_projector
from src.realtime.dispatcher import dispatcher
from src.realtime.ws import stream_request
from src.storage import create_request

@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(run_event_pump(stop, dispatcher.publish)),   # Kafka -> WS
        asyncio.create_task(run_status_projector(stop)),                 # Kafka -> Postgres
    ]
    yield
    stop.set()
    for t in tasks:
        t.cancel()
    await producer.stop()

@app.post("/index")
async def index(url: str):
    request_id = await producer.submit_index_request(url)
    await create_request(request_id, url)     # so the WS sees pending right away
    return {"request_id": str(request_id)}

@app.websocket("/ws/requests/{request_id}")
async def ws_request(ws: WebSocket, request_id: UUID):
    await stream_request(ws, request_id)
```

## Status over WebSocket

`request_id` is returned from `POST /index` before a `doc_id` exists: the document is not
in the DB yet, `fetch` creates it. That is why the subscription is keyed by `request_id`,
while `doc_id` arrives with the very first event (`stage=fetch`, `status=fetched`) —
followed by `chunked` and `indexed`.

```
POST /index ──► index.requests ──► [fetch] ──► [chunk] ──► [embed]
     │                                │            │           │
 request_id                           └────────────┴───────────┴──► index.events
     │                                                                │
     └──► WS /ws/requests/{request_id} ◄── dispatcher ◄── event_pump ◄─┘
                    ▲                                  (group per process)
            snapshot from request ◄── projector ◄──────────────────┘
                                      (shared group)
```

`index.events` has two independent readers, and their groups differ fundamentally:

| | group | why |
|---|---|---|
| `run_event_pump` | unique per process | every API replica needs **all** events: the WS client hangs off one of them |
| `run_status_projector` | shared `cater.events.projector` | **exactly one** instance writes an event into `request`, otherwise N replicas duplicate the write |

What goes into the socket:

```jsonc
{"type":"snapshot","request_id":"...","doc_id":null,"status":"pending","history":[]}
{"type":"status","stage":"fetch","status":"fetched","doc_id":"8a1f...","seq":41,"final":false}
{"type":"status","stage":"chunk","status":"chunked","doc_id":"8a1f...","seq":42,"final":false}
{"type":"status","stage":"embed","status":"indexed","doc_id":"8a1f...","seq":43,"final":true}
```

After `final: true` (`indexed`, `skipped`, `failed`) the server closes the connection.
While idle, a `{"type":"ping"}` is sent every `WS_HEARTBEAT_S` — otherwise a proxy tears
down the silent socket.

Three things that make this more than "forward Kafka into the WS":

- **The "HTTP returned request_id, the WS is not open yet" race.** `fetch` finishes in
  hundreds of milliseconds and the client opens the socket later — the event carrying
  `doc_id` would go nowhere. So the `dispatcher` keeps a buffer of recent events per
  `request_id` (`WS_BUFFER_TTL_S`, 15 minutes by default) and hands it over as a replay
  on subscribe.
- **Reconnects and a second replica.** The buffer lives in process memory, so the durable
  answer is the snapshot from the `request` table — filled in by the `projector`.
  The order inside `stream_request`: `subscribe` **before** reading the snapshot
  (otherwise an event arriving between the SELECT and the subscription is lost), and the
  resulting duplicates are removed by dedup on `event_id`.
- **A slow client.** `dispatcher.publish` never waits for the socket: a subscription has
  its own queue (`WS_QUEUE_MAXSIZE`), an overflow drops events and logs it. Otherwise one
  stuck browser would slow down the pump, and with it the lag of the whole `index.events`
  group.

`status` in the events uses the same values as `Content.status`
(`pending → fetched → chunked → indexed`, plus `skipped` and `failed`), so
`GET /index/{doc_id}` via `storage.load_content_status` and the WS never disagree.
The status ordering in `request` is protected by `events.supersedes`: a `fetched` that
arrives late does not roll back an already recorded `indexed`, but after `failed` a DLQ
replay may start the progress over.

The projector can be moved out of the API into its own process:

```bash
python -m src.kafka_service run-projector
```

`src/realtime/listener.py` (pg_notify `request_update`) is not needed in this design:
there is a single source of statuses — `index.events`. A second path into the same
`dispatcher` would produce duplicates and ordering divergence.

The `request` table comes from migration `b7c1d2e3f4a5` (`alembic upgrade head`). Its
`content_id` is `ON DELETE SET NULL`: delete the document and the request history stays.

## CDC: orphaned vectors in Qdrant

`Chunk.id` is exactly the point id in Qdrant (`indexer._build_points`), but deleting a
chunk row (including via a cascade from `Content`) knows nothing about Qdrant: the point
stays in the collection forever and surfaces in search. Application code does not fix
this — the database log does.

```
Postgres WAL ──► Debezium Server ──► cater.public.chunks ──► cdc.run_cdc_sync ──► Qdrant delete
   (chunks)        (slot cater_qdrant_sync)                  (batches of 512)
```

The indexing pipeline still runs on explicit messages (`index.requests` and onward) —
there it needs intent (`force`, `collection`, `request_id`), which table rows do not
carry. CDC is responsible for exactly one thing: deletions.

```bash
docker compose -f src/docker-compose.yml up -d postgres kafka debezium
python -m src.kafka_service run-cdc                    # the deletion consumer
python -m src.kafka_service sweep --dry-run            # how many orphans piled up before CDC
python -m src.kafka_service sweep                      # delete them
```

How it works:

- Debezium (`plugin.name=pgoutput`, built into PG 10+) holds a logical replication slot
  and publishes `public.chunks` events to `cater.public.chunks`. `column.exclude.list`
  drops `chunks.text` — the chunk body is not needed in the topic and would bloat it.
- The consumer takes `before.id` out of `op=d` events, accumulates them
  (`CDC_DELETE_BATCH=512`, `CDC_LINGER_S=2`) and hits Qdrant with a single request:
  deleting a document cascades into thousands of rows, hence thousands of events.
- Order: delete in Qdrant first, commit the offset second. Deleting by id is idempotent
  (a missing point is not an error), so a redelivery is safe.
- `CDC_COLLECTIONS` is empty by default → we hit every Qdrant collection: a chunk delete
  event does not know which collection it was indexed into. The list is cached for 60s.
- `REPLICA IDENTITY` needs no change: a delete event only needs the PK, and `content_id`
  is not required for deletion by id.

CDC pitfalls:

- **A slot is a disk risk.** While Debezium is down, Postgres does not clean up WAL; a
  forgotten slot fills the disk and takes the DB with it. Monitor `pg_replication_slots`
  and the size of `pg_wal`. If a slot is dead for good —
  `SELECT pg_drop_replication_slot('cater_qdrant_sync')`.
- **`heartbeat.interval.ms=30000` is mandatory.** If `chunks` is quiet while the database
  writes to other tables, the slot LSN does not move and WAL piles up even with a healthy
  Debezium.
- **`snapshot.mode=no_data`** — CDC only captures changes made after the slot was created.
  History is handled by `sweep`, which also covers gaps (TRUNCATE, a lost slot, restoring
  the DB from a dump). `TRUNCATE` cannot be synchronized through CDC at all: the event
  carries no row ids.
- **Debezium offsets live in the `debezium_data` volume.** Wipe the volume and the
  connector starts from the current WAL position, losing the deletes that happened while
  it was down (curable with `sweep`).
- **`wal_level=logical`** is set in the `command` of the postgres service; on an
  already-running container Postgres has to be restarted.

## Guarantees and pitfalls

**At-least-once, not exactly-once.** In the worker loop `producer.flush()` comes first,
`consumer.commit()` second. The reverse order would lose work: the offset has advanced
while the next stage's message never arrived. On a crash between flush and commit the
message is processed again — which is why every stage is idempotent:

- `fetch` — dedup by `content_hash` in `save_text` (a repeat returns the same `doc_id`);
- `chunk` / `embed` — exit immediately if `Content.status == indexed`;
- `embed` — a Qdrant upsert keyed by chunk id overwrites the point instead of duplicating it.

**The partitioning key is `doc_id`** (`url` in fetch). All messages of one document sit in
the same partition, so they are processed in order and without two workers racing over one
document.

**`max.poll.interval.ms`.** If a handler runs longer than the limit, the broker considers
the consumer dead and hands the partition to another one — and the document gets processed
twice in parallel. For `embed` the limit is raised to 30 minutes
(`KAFKA_EMBED_MAX_POLL_INTERVAL_MS`). In-stage retries eat the same budget:
`max_attempts` × backoff must fit inside it.

**DLQ.** Exhausted retries and malformed JSON go to `index.dlq` together with the original
body, the topic/partition/offset and the error text; the offset is committed as well — so
a stage does not get stuck forever on one message. To put it back to work:
`python -m src.kafka_service replay` (bumps `attempt` and puts it back into the source
topic).

**Partition count only goes up.** It cannot be decreased, and increasing it breaks the
key → partition mapping: messages of one document that are already stored may end up in
different partitions. Change it on empty topics or with the workers stopped.

**Compression caveat.** With confluent-kafka lz4 is baked into librdkafka, with aiokafka it
is not — hence `aiokafka[lz4]` in the dependencies.

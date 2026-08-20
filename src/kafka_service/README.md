# kafka_service

Kafka как основной транспорт задач индексации — вместо Celery. Каждая стадия
пайплайна это отдельная consumer-группа, стадии связаны топиками:

```
index.requests ──► [fetch]  ──► content.fetched ──► [chunk] ──► chunks.ready ──► [embed] ──► index.done
       ▲                │                  │                             │
   FastAPI /            └──────────────────┴─────────────────────────────┴──► index.events (статусы, WS)
   CLI submit                                                                 index.dlq    (падения)

Postgres WAL ──► Debezium ──► cater.public.chunks ──► [cdc] ──► удаление точек в Qdrant
```

Что где лежит: Postgres — источник правды (`Content`, `ContentText`, `Chunk`),
Qdrant — векторы, Kafka — только идентификаторы (`doc_id`, `url`). Тексты и векторы
через брокер не гоняем.

## Файлы

| Файл | Назначение |
|---|---|
| `config.py` | топики, группы, конфиги продюсера/консьюмера, тайминги (всё через env) |
| `schemas.py` | `Envelope[T]` + payload'ы: `IndexRequest`, `ContentFetched`, `ChunksReady`, `IndexDone`, `StatusEvent`, `StageFailed` |
| `stages.py` | обработчики стадий — чистые функции, Kafka не знают, тестируются напрямую |
| `worker.py` | цикл consume → handler → produce → commit, ретраи, DLQ, graceful shutdown |
| `sync_client.py` | confluent-kafka (librdkafka): продюсер + фабрика консьюмера — для воркеров |
| `async_client.py` | aiokafka: `producer.submit_index_request()` и мост `index.events` → WebSocket — для FastAPI |
| `events.py` | проекция `Envelope[StatusEvent]` в плоское событие для WS и для БД |
| `projector.py` | консьюмер `index.events` → таблица `request` (снапшоты для WS) |
| `admin.py` | явное создание топиков (авто-создание в брокере выключено) |
| `cdc.py` | CDC-синк Postgres → Qdrant: удаление осиротевших точек + разовый `sweep` |
| `debezium/application.properties` | конфиг Debezium Server (монтируется в контейнер) |
| `__main__.py` | CLI: `topics`, `run`, `submit`, `tail`, `replay`, `run-cdc`, `sweep` |

## Запуск

```bash
uv sync --group worker --group kafka          # воркеры (fetch/chunk/embed)
uv sync --group server --group kafka          # FastAPI

docker compose -f src/docker-compose.yml up -d kafka
python -m src.kafka_service topics            # создать топики

# по процессу на стадию (прод-раскладка)
python -m src.kafka_service run fetch
python -m src.kafka_service run chunk
python -m src.kafka_service run embed

# или всё в одном процессе — для локальной отладки
python -m src.kafka_service run fetch chunk embed --ensure-topics

python -m src.kafka_service run-projector      # статусы index.events -> таблица request

python -m src.kafka_service submit https://huggingface.co/blog/some-post
python -m src.kafka_service tail index.events
```

Масштабирование: поднять N процессов одной стадии с той же `group.id` — Kafka сама
раздаст партиции. Потолок параллелизма = число партиций (`KAFKA_TOPIC_PARTITIONS`, по умолчанию 6).

## Подключение к FastAPI

Модуль ничего не патчит в `main.py` — вот минимальная обвязка:

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
    await create_request(request_id, url)     # чтобы WS сразу увидел pending
    return {"request_id": str(request_id)}

@app.websocket("/ws/requests/{request_id}")
async def ws_request(ws: WebSocket, request_id: UUID):
    await stream_request(ws, request_id)
```

## Статус по WebSocket

`request_id` возвращается из `POST /index` до того, как существует `doc_id`:
документа в БД ещё нет, его создаёт `fetch`. Поэтому подписка идёт по
`request_id`, а `doc_id` приезжает первым же событием (`stage=fetch`,
`status=fetched`) — дальше `chunked`, `indexed`.

```
POST /index ──► index.requests ──► [fetch] ──► [chunk] ──► [embed]
     │                                │            │           │
 request_id                           └────────────┴───────────┴──► index.events
     │                                                                │
     └──► WS /ws/requests/{request_id} ◄── dispatcher ◄── event_pump ◄─┘
                    ▲                                    (группа на процесс)
              snapshot из request ◄── projector ◄────────────────────┘
                                      (общая группа)
```

У `index.events` два независимых читателя, и группы у них разные принципиально:

| | группа | зачем |
|---|---|---|
| `run_event_pump` | уникальная на процесс | каждой реплике API нужны **все** события: WS-клиент висит на одной из них |
| `run_status_projector` | общая `cater.events.projector` | событие пишет в `request` **ровно один** инстанс, иначе N реплик дублируют запись |

Что отдаётся в сокет:

```jsonc
{"type":"snapshot","request_id":"...","doc_id":null,"status":"pending","history":[]}
{"type":"status","stage":"fetch","status":"fetched","doc_id":"8a1f...","seq":41,"final":false}
{"type":"status","stage":"chunk","status":"chunked","doc_id":"8a1f...","seq":42,"final":false}
{"type":"status","stage":"embed","status":"indexed","doc_id":"8a1f...","seq":43,"final":true}
```

После `final: true` (`indexed`, `skipped`, `failed`) сервер закрывает соединение.
В простое летит `{"type":"ping"}` раз в `WS_HEARTBEAT_S` — иначе прокси рвёт
молчащий сокет.

Три вещи, из-за которых это не просто «форвардить Kafka в WS»:

- **Гонка «HTTP отдал request_id, WS ещё не открыт».** `fetch` укладывается в
  сотни миллисекунд, клиент открывает сокет позже — событие с `doc_id` ушло бы
  в никуда. Поэтому `dispatcher` держит буфер последних событий по `request_id`
  (`WS_BUFFER_TTL_S`, по умолчанию 15 минут) и отдаёт его при подписке как replay.
- **Реконнект и вторая реплика.** Буфер живёт в памяти процесса, поэтому durable
  ответ даёт снапшот из таблицы `request` — её наполняет `projector`.
  Порядок в `stream_request`: `subscribe` **до** чтения снапшота (иначе событие,
  пришедшее между SELECT и подпиской, теряется), а дубли снимаются дедупом
  по `event_id`.
- **Медленный клиент.** `dispatcher.publish` не ждёт сокет: у подписки своя
  очередь (`WS_QUEUE_MAXSIZE`), переполнение теряет события и пишет в лог. Иначе
  один залипший браузер тормозил бы pump, а с ним и лаг всей группы `index.events`.

`status` в событиях — те же значения, что в `Content.status`
(`pending → fetched → chunked → indexed`, плюс `skipped` и `failed`), так что
`GET /index/{doc_id}` через `storage.load_content_status` и WS не расходятся.
Порядок статусов в `request` защищён `events.supersedes`: пришедшее позже
`fetched` не откатывает уже записанный `indexed`, но после `failed` реплей из DLQ
может начать прогресс заново.

Проектор можно вынести из API отдельным процессом:

```bash
python -m src.kafka_service run-projector
```

`src/realtime/listener.py` (pg_notify `request_update`) при этой схеме не нужен:
источник статусов один — `index.events`. Второй путь в тот же `dispatcher` дал бы
дубли и расхождение порядка.

Таблица `request` — миграция `b7c1d2e3f4a5` (`alembic upgrade head`). `content_id`
там `ON DELETE SET NULL`: удалили документ — история запроса остаётся.

## CDC: осиротевшие векторы в Qdrant

`Chunk.id` — это ровно id точки в Qdrant (`indexer._build_points`), но удаление строки
чанка (в том числе каскадом от `Content`) про Qdrant ничего не знает: точка остаётся в
коллекции навсегда и всплывает в поиске. Прикладной код это не чинит — чинит журнал БД.

```
Postgres WAL ──► Debezium Server ──► cater.public.chunks ──► cdc.run_cdc_sync ──► Qdrant delete
   (chunks)        (слот cater_qdrant_sync)                   (батч по 512)
```

Пайплайн индексации по-прежнему ходит по явным сообщениям (`index.requests` и далее) —
там нужен intent (`force`, `collection`, `request_id`), которого в строках таблицы нет.
CDC отвечает только за одно: удаления.

```bash
docker compose -f src/docker-compose.yml up -d postgres kafka debezium
python -m src.kafka_service run-cdc                    # консьюмер удалений
python -m src.kafka_service sweep --dry-run            # сколько сирот накопилось до CDC
python -m src.kafka_service sweep                      # удалить их
```

Как это работает:

- Debezium (`plugin.name=pgoutput`, встроен в PG 10+) держит слот логической репликации
  и публикует события `public.chunks` в `cater.public.chunks`. `column.exclude.list`
  выкидывает `chunks.text` — тело чанка в топике не нужно и раздуло бы его.
- Консьюмер берёт `before.id` из событий `op=d`, копит их (`CDC_DELETE_BATCH=512`,
  `CDC_LINGER_S=2`) и бьёт одним запросом в Qdrant: удаление документа каскадит в тысячи
  строк, значит и в тысячи событий.
- Порядок: сначала удаление в Qdrant, потом коммит оффсета. Удаление по id идемпотентно
  (нет точки — не ошибка), поэтому повторная доставка безопасна.
- `CDC_COLLECTIONS` пуст по умолчанию → бьём по всем коллекциям Qdrant: событие удаления
  чанка не знает, в какую коллекцию его индексировали. Список кешируется на 60с.
- `REPLICA IDENTITY` менять не нужно: для delete-события хватает PK, а `content_id`
  для удаления по id не требуется.

Подводные камни CDC:

- **Слот — это риск для диска.** Пока Debezium лежит, Postgres не чистит WAL; забытый слот
  забивает диск и роняет БД. Мониторить `pg_replication_slots` и размер `pg_wal`.
  Слот мёртв навсегда — `SELECT pg_drop_replication_slot('cater_qdrant_sync')`.
- **`heartbeat.interval.ms=30000` обязателен.** Если в `chunks` тихо, а база пишет в другие
  таблицы, LSN слота не двигается и WAL копится даже при живом Debezium.
- **`snapshot.mode=no_data`** — CDC ловит только изменения с момента создания слота.
  Историю разгребает `sweep`, он же страхует пропуски (TRUNCATE, потерянный слот, залив
  БД из дампа). `TRUNCATE` через CDC не синхронизировать в принципе: id строк в событии нет.
- **Оффсеты Debezium живут в томе `debezium_data`.** Снесёшь том — коннектор начнёт с
  текущей позиции WAL, и удаления, случившиеся в простое, потеряются (лечится `sweep`).
- **`wal_level=logical`** прописан в `command` сервиса postgres; на уже поднятом контейнере
  требуется рестарт Postgres.

## Гарантии и подводные камни

**At-least-once, не exactly-once.** В цикле воркера сначала `producer.flush()`,
потом `consumer.commit()`. Обратный порядок терял бы работу: оффсет сдвинут, а
сообщение следующей стадии не долетело. При падении между flush и commit сообщение
обработается повторно — поэтому все стадии идемпотентны:

- `fetch` — дедуп по `content_hash` в `save_text` (повтор вернёт тот же `doc_id`);
- `chunk` / `embed` — выходят сразу, если `Content.status == indexed`;
- `embed` — Qdrant upsert по id чанка перезаписывает точку, а не дублирует.

**Ключ партиционирования = `doc_id`** (в fetch — `url`). Все сообщения одного
документа лежат в одной партиции, значит обрабатываются по порядку и без гонки
двух воркеров за один документ.

**`max.poll.interval.ms`.** Если handler работает дольше лимита, брокер считает
консьюмера мёртвым, отдаёт партицию другому — и документ обрабатывается дважды
параллельно. У `embed` лимит поднят до 30 минут (`KAFKA_EMBED_MAX_POLL_INTERVAL_MS`).
Ретраи внутри стадии съедают тот же бюджет: `max_attempts` × backoff должны в него влезать.

**DLQ.** Исчерпанные ретраи и битый JSON уходят в `index.dlq` вместе с исходным телом,
топиком/партицией/оффсетом и текстом ошибки; оффсет при этом коммитится — стадия не
встаёт колом на одном сообщении. Вернуть в работу: `python -m src.kafka_service replay`
(поднимает `attempt` и кладёт обратно в исходный топик).

**Число партиций меняется только вверх.** Уменьшить нельзя, а увеличение ломает
привязку ключ → партиция: уже лежащие сообщения одного документа могут оказаться
в разных партициях. Менять на пустых топиках или с остановленными воркерами.

**Порядок компрессии.** У confluent-kafka lz4 вшит в librdkafka, у aiokafka — нет,
поэтому в зависимостях стоит `aiokafka[lz4]`.

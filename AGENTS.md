Кодовая база в процессе глобального рефакторинга

# Рефакторинг: Разделение по слоям

## Context

Цель: явные слои (domain → repository/client → service → api), где внешние зависимости (sqlx, tonic/prost, axum) не просачиваются за пределы своего слоя. Рефакторинг начат, кодовая база в поломанном промежуточном состоянии.

## Диаграмма слоёв

```
domain (никаких внешних зав-тей кроме serde/sha2/tokio/postcard)
  ↓              ↓
repository     client/          <- инфраструктура: хранилище и исходящий транспорт
(sqlx/FS)     (tonic-client)    <- оба на одном уровне, независимы друг от друга
  ↓              ↓
service  ←────────              <- бизнес-логика, использует репо и клиент через трейты
  ↓
api/http + api/grpc              <- HTTP-сервер и gRPC-сервер
  ↓
node                             <- точка компоновки, дженерики
```

## Target Structure

```
crates/core/src/
├── domain/
│   ├── error.rs                     # Убрать From<SqlxError>
│   ├── command.rs                   # ObjectCommand, CommandResult — без изменений
│   └── consensus/
│       ├── clock.rs                 # ✓ готово
│       ├── command_id.rs            # ПЕРЕИМЕНОВАТЬ из command.rs: CommandId, DependencySet
│       ├── state_machine.rs         # ПЕРЕНЕСТИ из consensus/state_machine.rs
│       └── transport.rs             # НОВЫЙ: domain-типизированные запросы/ответы + трейты
│
├── repository/
│   ├── blob/, metadata/, applied_command/  # Добавить локальные sqlx→So3Error маппинги
│   ├── consensus_journal/           # ПЕРЕНЕСТИ из consensus/journal.rs
│   │   ├── interface.rs             # ConsensusJournal трейт
│   │   └── sqlite.rs                # postcard, schema v5
│   └── registry.rs                  # RepositoryRegistry — обновить импорты
│
├── client/                          # Исходящий transport (тот же уровень что repository)
│   └── consensus_peer.rs            # ConsensusPeerGrpcClient
│                                    # Реализует domain::consensus::transport::ConsensusPeerTransport
│                                    # Использует proto.rs из api/grpc (или из общего proto/)
│
├── service/
│   ├── consensus/                   # ПЕРЕНЕСТИ из consensus/ модуля
│   │   ├── coordinator.rs           # domain-typed transport (без proto)
│   │   ├── executor.rs
│   │   └── recovery.rs
│   ├── object/
│   │   ├── interface.rs             # ObjectService трейт
│   │   └── service.rs               # ПЕРЕНЕСТИ из object_server/service.rs
│   └── registry.rs                  # ServiceRegistry<E, B> — агрегирует сервисы
│
├── api/
│   ├── http/                        # ПЕРЕИМЕНОВАТЬ из object_server/
│   │   ├── controller.rs            # Использует service::object::ObjectService
│   │   ├── api.rs
│   │   └── server.rs
│   └── grpc/                        # ПЕРЕИМЕНОВАТЬ из rpc_server/
│       ├── proto.rs                 # Сгенерированный код (используется и client/, и здесь)
│       ├── server.rs
│       ├── service.rs               # gRPC boundary: proto↔domain конвертация
│       └── transport/
│           ├── applying.rs          # Реализует domain ConsensusTransportHandler
│           └── rejecting.rs         # Реализует domain ConsensusTransportHandler
│
└── node/
    └── runtime.rs                   # Node<E, P, B> — дженерик, точка компоновки
```

**Модуль `consensus/` полностью растворяется.**

## Ключевые решения

### A. Именование command-типов

- `domain/command.rs` — операции над объектами (`ObjectCommand`, `CasResult`) — **остаётся**
- `domain/consensus/command.rs` → **`command_id.rs`**: идентификаторы протокола (`CommandId`, `DependencySet`)

### B. Domain-типизированные transport-трейты (`domain/consensus/transport.rs`)

```rust
pub struct Ballot { pub round: u64, pub node_id: String }
pub struct PreAcceptRequest { pub command_id: CommandId, pub command: Vec<u8>, pub timestamp_zero: LogicalTimestamp }
pub struct PreAcceptResponse { pub timestamp: Option<LogicalTimestamp>, pub dependencies: DependencySet, pub nack: bool }
// ... AcceptRequest/Response, CommitRequest/Response, RecoverRequest/Response, ApplyRequest/Response, FetchBlobRequest/Response

#[async_trait]
pub trait ConsensusTransportHandler: Send + Sync {
    async fn pre_accept(&self, request: PreAcceptRequest) -> So3Result<PreAcceptResponse>;
    // ...
}

#[async_trait]
pub trait ConsensusPeerTransport: Send {
    async fn pre_accept_peer(&mut self, peer_id: &str, request: PreAcceptRequest) -> So3Result<PreAcceptResponse>;
    // ...
}
```

### C. Client модуль (`client/consensus_peer.rs`)

- `ConsensusPeerGrpcClient` (переименован из `TonicConsensusPeerTransport`)
- Находится на том же уровне что `repository/` — это outbound-инфраструктура
- Реализует `domain::consensus::transport::ConsensusPeerTransport`
- Конвертирует domain → proto перед вызовом, proto → domain при ответе
- `map_tonic_status()` живёт здесь
- Используется `proto.rs` из `api/grpc/proto.rs` — `client/` зависит от `api/grpc/proto`, но **не от `api/grpc/service`**
- Тесты из `tonic_peer.rs` переезжают сюда

### D. Generics для Node

`Node` становится дженериком по executor, peer transport и blob repository:

```rust
pub struct Node<E, P, B>
where
    E: ReplicatedCommandExecutor + Clone + Send + Sync + 'static,
    P: ConsensusPeerTransport + Clone + Send + Sync + 'static,
    B: BlobRepository + Clone + Send + Sync + 'static,
{
    config: NodeConfig,
    rpc_server: ApiGrpcServer<ApplyingConsensusTransport<E>>,
    object_service: ObjectServiceImpl<LocalConsensusObjectCommandExecutor<ApplyingConsensusTransport<E>, P>, B>,
}

// Фабрика для стандартной конфигурации (SQLite + FS + gRPC):
type DefaultNode = Node<
    PersistentReplicatedCommandExecutor<SqliteFsPersistentObjectRepository, SqliteObjectMetadataRepository>,
    ConsensusPeerGrpcClient,
    FileSystemBlobRepository,
>;

impl DefaultNode {
    pub async fn from_config(config: NodeConfig) -> So3Result<Self> { ... }
}
```

### E. ServiceRegistry

```rust
pub struct ServiceRegistry<E, B>
where
    E: ObjectCommandExecutor + Clone + Send + Sync + 'static,
    B: BlobRepository + Clone + Send + Sync + 'static,
{
    pub object_service: ObjectServiceImpl<E, B>,
}
```

### F. Journal: prost → postcard + schema v5

Добавить `#[derive(Serialize, Deserialize)]` к `CommandId`, `LogicalTimestamp`, `Ballot`.  
Заменить prost-encode/decode на postcard. Schema v5 = DROP+CREATE (uncommitted-данные восстанавливаются через Accord recovery).

## Шаги реализации

### Шаг 1: Завершить начатую миграцию ConsensusCommandId

Переименовать `domain/consensus/command.rs` → `command_id.rs`.  
Заменить `ConsensusCommandId` → `CommandId` (`domain::consensus::command_id::CommandId`) в 9 файлах:  
`consensus/{coordinator,executor,journal,recovery}.rs`, `rpc_server/transport/applying.rs`, `repository/{registry,applied_command/sqlite,applied_command/interface}.rs`, `node/runtime.rs`

**Проверка:** `cargo clippy -- -W clippy::pedantic`

### Шаг 2: Убрать sqlx из domain/error.rs

- `domain/error.rs` — удалить `use sqlx::Error as SqlxError` и `impl From<SqlxError>`
- на уровне репозиториев сделать отдельный `impl From<SqlxError>` для доменного типа ошибки

**Проверка:** `cargo clippy -- -W clippy::pedantic`

### Шаг 3: Перенести state_machine.rs в domain/consensus/

- `consensus/state_machine.rs` → `domain/consensus/state_machine.rs`
- Обновить `domain/consensus/mod.rs`; удалить из `consensus/mod.rs`
- Обновить импорты в `consensus/executor.rs`, `object_server/service.rs`, `rpc_server/transport/applying.rs`

**Проверка:** `cargo clippy -- -W clippy::pedantic`

### Шаг 4: Domain transport-типы + отвязать coordinator от proto

**4a.** Создать `domain/consensus/transport.rs` с domain-типами и двумя трейтами (см. раздел B).

**4b.** `rpc_server/transport/handler.rs` — `ConsensusTransportHandler` становится ре-экспортом из `domain::consensus::transport`.

**4c.** `rpc_server/transport/{applying,rejecting}.rs`:

- Методы возвращают `So3Result<_>` (не `Result<_, tonic::Status>`)
- Принимают domain-типы (`PreAcceptRequest`, не `proto::PreAcceptRequest`)
- `map_error` (So3Error→Status) переносится в `rpc_server/service.rs`

**4d.** `rpc_server/service.rs` (gRPC boundary):

- Конвертирует proto ↔ domain типы в каждом методе
- Конвертирует `So3Error → tonic::Status` только здесь

**4e.** `consensus/coordinator.rs`:

- Убрать `use tonic::Status`, `use crate::rpc_server::proto::*`
- Убрать `command_id_proto()`, `map_status()`
- `RecoveryDecision.wait_for: Vec<proto::CommandId>` → `Vec<CommandId>` из domain
- Тесты: `FakeLocalTransport` и `FakePeerTransport` реализуют domain-трейты

**Проверка:** `cargo clippy -- -W clippy::pedantic -D warnings`, затем `cargo test -p so3-core`

### Шаг 5: Растворить consensus/ → repository + service

**5a. Journal → repository:**

- Создать `repository/consensus_journal/interface.rs` — `ConsensusJournal` трейт
- Создать `repository/consensus_journal/sqlite.rs` — postcard, schema v5, local sqlx_err
- Добавить `#[derive(Serialize, Deserialize)]` к `CommandId`, `LogicalTimestamp`, `Ballot`
- Обновить `repository/registry.rs` — импорт журнала из нового места
- Удалить `consensus/journal.rs`

**5b. Coordinator, executor, recovery → service/consensus/:**

- Переместить содержимое (не копировать) в `service/consensus/`
- Обновить внутренние импорты (`crate::consensus::` → `crate::service::consensus::`, `crate::repository::consensus_journal::`, `crate::domain::consensus::`)
- Удалить `consensus/` модуль целиком; убрать из `lib.rs`

**Проверка:** `cargo clippy -- -W clippy::pedantic -D warnings`, затем `cargo test -p so3-core`

### Шаг 6: Выделить client-модуль

- Создать `client/consensus_peer.rs` — `ConsensusPeerGrpcClient` из `rpc_server/transport/tonic_peer.rs`
  - Реализует `domain::consensus::transport::ConsensusPeerTransport`
  - Конвертирует domain → proto → domain; `map_tonic_status()` здесь
  - Зависит от `api/grpc/proto.rs` для типов сгенерированного кода
- Создать `client/mod.rs` с `pub mod consensus_peer;`
- Обновить `service/consensus/executor.rs` — импорт `ConsensusPeerGrpcClient` из `client::`
- Удалить `rpc_server/transport/tonic_peer.rs`

**Проверка:** `cargo clippy -- -W clippy::pedantic`

### Шаг 7: ObjectService + ServiceRegistry + переименование api-модулей

**7a.** `object_server/service.rs` → `service/object/service.rs`; согласовать с `service/object/interface.rs`.

**7b.** `service/registry.rs` — заполнить `ServiceRegistry<E, B>` (см. раздел E).

**7c.** Переименовать:

- `object_server/` → `api/http/`
- `rpc_server/` → `api/grpc/`
- Создать `api/mod.rs`
- Обновить `lib.rs`, `node/runtime.rs`

**Проверка:** `cargo clippy -- -W clippy::pedantic -D warnings`, затем `cargo test -p so3-core`

### Шаг 8: Дженерики для Node

Сделать `Node<E, P, B>` дженериком (см. раздел D).  
Добавить `DefaultNode` type alias и `DefaultNode::from_config(config: NodeConfig)` фабрику.  
Существующий `Node::new(config)` переименовать в `DefaultNode::from_config(config)`.

**Проверка:** `cargo clippy -- -W clippy::pedantic`, затем `cargo test -p so3-core`

### Шаг 9: Обновить so3

`crates/so3/src/main.rs` — заменить `Node::new(config)` на `DefaultNode::from_config(config)`.  
Обновить пути импортов если они зависели от переименованных модулей.

**Проверка:** `cargo clippy -- -W clippy::pedantic`, затем `cargo build -p so3`

### Шаг 10: Обновить so3-maelstrom

**Ситуация:** `MaelstromPeerTransport` реализует `ConsensusPeerTransport` через Maelstrom JSON-канал.  
Сейчас методы трейта принимают proto-типы и prost-кодируют payload для отправки.  
После рефакторинга трейт использует domain-типы.

**Изменения в `runtime.rs`:**

1. Убрать `use prost::Message as ProstMessage`
2. Убрать `use so3_core::rpc_server::proto::*`
3. `MaelstromPeerTransport` реализует `domain::consensus::transport::ConsensusPeerTransport`
4. В методах — принимать domain request, кодировать через `postcard::to_allocvec()` для payload, декодировать через `postcard::from_bytes()` (domain-типы имеют serde, postcard используется в проекте)
5. `decode_proto::<T>()` → `decode_postcard::<T>()`
6. `status_error()` → убрать (возвращаем `So3Error` напрямую)
7. `handle_consensus()` — `local_transport.pre_accept(req)` теперь принимает domain-тип; при обработке входящих RPC декодировать postcard → domain, вызвать handler, кодировать ответ обратно в postcard

8. Обновить `ConsensusCommandId::new(...)` → `CommandId::new(NodeId::from(...), ...)`
9. Обновить `so3_core::consensus::*` → `so3_core::service::consensus::*` / `so3_core::domain::consensus::*`

**Изменения в `service.rs`:**

- `so3_core::consensus::state_machine::ObjectCommandExecutor` → `so3_core::domain::consensus::state_machine::ObjectCommandExecutor`
- `so3_core::object_server::service::ObjectService` → `so3_core::service::object::service::ObjectServiceImpl`

**Важно:** Смена кодировки payload (proto → postcard) **ломает wire-совместимость** с существующими узлами.  
Поскольку это тестовый харнесс, а не продакшн-протокол, это приемлемо.

**Проверка:** `cargo clippy -- -W clippy::pedantic`, затем `cargo test -p so3-maelstrom`

## Проверка завершённости

```bash
# Пошаговая проверка (после каждого шага)
cargo clippy -- -W clippy::pedantic -D warnings
cargo test -p so3-core

# Финальная проверка слоёв
grep -r "sqlx" crates/core/src/domain/          # → 0
grep -r "tonic\|prost" crates/core/src/domain/   # → 0
grep -r "tonic\|prost" crates/core/src/service/  # → 0
grep -r "sqlx" crates/core/src/service/          # → 0
grep -r "ConsensusCommandId" crates/core/src/    # → 0
grep -r "rpc_server\|object_server" crates/core/src/ # → 0
grep -r "consensus::" crates/core/src/           # → 0 (модуль удалён)

# Финальная функциональная проверка (Maelstrom не работает на Windows)
scripts/maelstrom/smoke-3-node-lin-kv.sh
scripts/maelstrom/fault-3-node-lin-kv.sh
scripts/k6/run-benchmark.sh
```

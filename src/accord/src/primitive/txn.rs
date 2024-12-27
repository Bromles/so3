struct TxnKindData {
    short_name: char,
    is_visible: bool,
    is_sync_point: bool,
    is_system: bool,
    await_only_deps: bool,
}

enum TxnKind {
    Read,
    Write,
    EphemeralRead,
    SyncPoint,
    ExclusiveSyncPoint,
}

impl TxnKind {
    pub fn value(&self) -> TxnKindData {
        match self {
            TxnKind::Read => TxnKindData {
                short_name: 'R',
                is_visible: true,
                is_sync_point: false,
                is_system: false,
                await_only_deps: false,
            },
            TxnKind::Write => TxnKindData {
                short_name: 'W',
                is_visible: true,
                is_sync_point: false,
                is_system: false,
                await_only_deps: false,
            },
            TxnKind::EphemeralRead => TxnKindData {
                short_name: 'E',
                is_visible: false,
                is_sync_point: false,
                is_system: false,
                await_only_deps: true,
            },
            TxnKind::SyncPoint => TxnKindData {
                short_name: 'S',
                is_visible: true,
                is_sync_point: true,
                is_system: true,
                await_only_deps: false,
            },
            TxnKind::ExclusiveSyncPoint => TxnKindData {
                short_name: 'X',
                is_visible: true,
                is_sync_point: true,
                is_system: true,
                await_only_deps: true,
            }
        }
    }
}

pub trait Txn {}

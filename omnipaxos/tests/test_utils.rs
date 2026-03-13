use omnipaxos::storage::{Entry, Snapshot};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Value {
    pub id: u64,
}

impl Value {
    pub fn with_id(id: u64) -> Self {
        Value { id }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ValueSnapshot {
    pub map: HashMap<u64, u64>,
}

impl Snapshot<Value> for ValueSnapshot {
    fn create(entries: &[Value]) -> Self {
        let mut map = HashMap::new();
        for e in entries {
            map.insert(e.id, e.id);
        }
        ValueSnapshot { map }
    }

    fn merge(&mut self, delta: Self) {
        self.map.extend(delta.map);
    }

    fn use_snapshots() -> bool {
        true
    }
}

impl Entry for Value {
    type Snapshot = ValueSnapshot;
}

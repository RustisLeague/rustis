use std::cmp::Ordering;

pub type Key = String;

#[derive(Clone, Eq, PartialEq)]
pub struct ExpireTime {
    key:Key,
    expire_at:u64,
}

impl Ord for ExpireTime {
    fn cmp(&self, other: &ExpireTime) -> Ordering {
        other.expire_at.cmp(&self.expire_at)
    }
}

impl PartialOrd for ExpireTime {
    fn partial_cmp(&self, other: &ExpireTime) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

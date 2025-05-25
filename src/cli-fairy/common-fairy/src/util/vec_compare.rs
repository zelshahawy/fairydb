use std::collections::HashSet;
use std::hash::Hash;

pub fn compare_unordered<T: Eq + Hash>(a: &[T], b: &[T]) -> bool {
    let mut a_set: HashSet<&T> = HashSet::new();
    let mut b_set: HashSet<&T> = HashSet::new();
    for a_elem in a {
        a_set.insert(a_elem);
    }
    for b_elem in b {
        b_set.insert(b_elem);
    }
    a_set == b_set
}

pub fn compare_ordered<T: Eq>(a: &[T], b: &[T]) -> bool {
    a == b
}

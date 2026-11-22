//! Choosing which orphans fsck should link into `lost+found`.

use std::collections::{HashSet, VecDeque};

/// The orphans to adopt: those not inside another adopted orphan directory, directories first.
/// Adopting a directory makes its contents reachable again, so linking them too would
/// double-link them. `children` lists a directory's entries without `.` and `..`; of orphan
/// directories that contain each other, the first met is kept.
pub(crate) fn orphan_roots(
    orphans: &[(u32, bool)],
    mut children: impl FnMut(u32) -> Vec<u32>,
) -> Vec<u32> {
    let mut covered: HashSet<u32> = HashSet::new();
    let mut roots = Vec::new();
    let dirs = orphans.iter().filter(|o| o.1);
    let files = orphans.iter().filter(|o| !o.1);
    for &(inum, is_dir) in dirs.chain(files) {
        if covered.contains(&inum) {
            continue;
        }
        roots.push(inum);
        covered.insert(inum);
        if !is_dir {
            continue;
        }
        let mut queue = VecDeque::from([inum]);
        while let Some(dir) = queue.pop_front() {
            for child in children(dir) {
                if covered.insert(child) {
                    queue.push_back(child);
                }
            }
        }
    }
    roots
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_orphan_directorys_contents_are_not_adopted_again() {
        // 10 is an orphan dir holding 11 (file) and 12 (dir holding 13); 20 stands alone.
        let tree = |d: u32| match d {
            10 => vec![11, 12],
            12 => vec![13],
            _ => vec![],
        };
        let orphans = [
            (11, false),
            (10, true),
            (13, false),
            (12, true),
            (20, false),
        ];
        assert_eq!(orphan_roots(&orphans, tree), [10, 20]);
    }
}

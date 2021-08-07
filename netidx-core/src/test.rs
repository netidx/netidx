use crate::path::{self, Path};
use fxhash::FxHashMap;
use rand::{prelude::*, thread_rng};
use std::time::{Duration, Instant};

#[test]
fn test_path_hc() {
    {
        const SIZE: usize = 10_000;
        const ITER: usize = 100;
        let mut expected_total = 0;
        let mut paths = (0..SIZE)
            .into_iter()
            .map(|i| {
                expected_total += i;
                Path::from(format!("/root/app/table/{}/column", i))
            })
            .collect::<Vec<_>>();
        assert_eq!(path::count(), paths.len());
        let table = paths
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, p)| (p, i))
            .collect::<FxHashMap<_, _>>();
        assert_eq!(path::count(), paths.len());
        let mut total_elapsed = Duration::from_secs(0);
        for _ in 0..100 {
            paths.shuffle(&mut thread_rng());
            let now = Instant::now();
            let mut total = 0;
            for path in paths.iter() {
                total += table[path];
            }
            assert_eq!(total, expected_total);
            total_elapsed += now.elapsed();
        }
        println!("{} ns", total_elapsed.as_nanos() / ((SIZE * ITER) as u128));
    }
    assert_eq!(path::count(), 0);
}

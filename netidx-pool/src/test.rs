use super::{
    arc::{Arc, TArc},
    Pool, RawPool,
};

/*
==829625== LEAK SUMMARY:
==829625==    definitely lost: 0 bytes in 0 blocks
==829625==    indirectly lost: 0 bytes in 0 blocks
==829625==      possibly lost: 412 bytes in 2 blocks
==829625==    still reachable: 320 bytes in 8 blocks
==829625==         suppressed: 0 bytes in 0 blocks
==829625== Reachable blocks (those to which a pointer was found) are not shown.
==829625== To see them, rerun with: --leak-check=full --show-leak-kinds=all
==829625==
==829625== For lists of detected and suppressed errors, rerun with: -s
==829625== ERROR SUMMARY: 2 errors from 2 contexts (suppressed: 0 from 0)

The two errors are the static memory in the pool shark thread and the
hashmap used to communicate with it.
*/
#[test]
fn normal_pool() {
    for _ in 0..100 {
        let pool: Pool<Vec<usize>> = Pool::new(1024, 1024);
        let mut v0 = pool.take();
        let mut v1 = pool.take();
        v0.reserve(100);
        v1.reserve(100);
        let (v0a, v1a) = (v0.as_ptr().addr(), v1.as_ptr().addr());
        let (v0c, v1c) = (v0.capacity(), v1.capacity());
        for _ in 0..100 {
            drop(v0);
            drop(v1);
            v0 = pool.take();
            v1 = pool.take();
            assert_eq!(v0.as_ptr().addr(), v0a);
            assert_eq!(v1.as_ptr().addr(), v1a);
            assert_eq!(v0.capacity(), v0c);
            assert_eq!(v1.capacity(), v1c);
            assert_eq!(v0.len(), 0);
            assert_eq!(v1.len(), 0);
            for i in 0..100 {
                v0.push(i);
                v1.push(i);
            }
            assert_eq!(pool.try_take(), None);
        }
        // vectors larger than 1024 will not be saved in the pool
        for _ in 0..100 {
            assert_eq!(pool.try_take(), None);
            let mut v2 = pool.take();
            assert_eq!(v2.capacity(), 0);
            v2.reserve(1025);
            for i in 0..1025 {
                v2.push(i);
            }
        }
        // add to pool
        drop(v0);
        // add to pool
        drop(v1);
        // should drop everything in the pool run under valgrind leak
        // check to ensure both v0 and v1 are actually freed
        drop(pool);
    }
}

/*
==831358== LEAK SUMMARY:
==831358==    definitely lost: 0 bytes in 0 blocks
==831358==    indirectly lost: 0 bytes in 0 blocks
==831358==      possibly lost: 412 bytes in 2 blocks
==831358==    still reachable: 328 bytes in 8 blocks
==831358==         suppressed: 0 bytes in 0 blocks
==831358== Reachable blocks (those to which a pointer was found) are not shown.
*/
#[test]
fn tarc_pool() {
    for _ in 0..100 {
        let pool: RawPool<TArc<String>> = RawPool::new(1024, 1);
        let mut v0 = TArc::new(&pool, "0".to_string());
        let mut v1 = TArc::new(&pool, "0".to_string());
        let v0a = v0.as_ptr().addr();
        let v1a = v1.as_ptr().addr();
        for i in 0..100 {
            drop(v0);
            drop(v1);
            v0 = TArc::new(&pool, i.to_string());
            v1 = TArc::new(&pool, i.to_string());
            assert_eq!(v0.as_ptr().addr(), v0a);
            assert_eq!(v1.as_ptr().addr(), v1a);
            assert_eq!(pool.try_take(), None);
            let v2 = v0.clone();
            let v3 = v1.clone();
            // drops v0 and v1, but they won't go back into the pool
            // because strong_count > 1.
            v0 = v2;
            v1 = v3;
            assert_eq!(pool.try_take(), None);
        }
        drop(v0);
        drop(v1);
        drop(pool)
    }
}

#[test]
fn arc_pool() {
    for _ in 0..100 {
        let pool: RawPool<Arc<String>> = RawPool::new(1024, 1);
        let mut v0 = Arc::new(&pool, "0".to_string());
        let mut v1 = Arc::new(&pool, "0".to_string());
        let v0a = v0.as_ptr().addr();
        let v1a = v1.as_ptr().addr();
        for i in 0..100 {
            drop(v0);
            drop(v1);
            v0 = Arc::new(&pool, i.to_string());
            v1 = Arc::new(&pool, i.to_string());
            assert_eq!(v0.as_ptr().addr(), v0a);
            assert_eq!(v1.as_ptr().addr(), v1a);
            assert_eq!(pool.try_take(), None);
            let v2 = v0.clone();
            let v3 = v1.clone();
            // drops v0 and v1, but they won't go back into the pool
            // because strong_count > 1.
            v0 = v2;
            v1 = v3;
            assert_eq!(pool.try_take(), None);
        }
        drop(v0);
        drop(v1);
        drop(pool)
    }
}

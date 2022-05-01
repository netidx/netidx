use super::store::Store;
use crate::{
    pack::Z64,
    path::Path,
    protocol::resolver::{HashMethod, Publisher, PublisherId, PublisherRef, TargetAuth},
};
use fxhash::FxHashMap;
use bytes::Bytes;
use rand::{self, Rng};
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::Arc,
};

#[test]
fn test_resolver_store() {
    let mut publishers: FxHashMap<SocketAddr, Arc<Publisher>> = HashMap::default();
    let mut hm = HashMap::new();
    hm.insert(Path::from("foo"), 0);
    assert_eq!(hm.get(&Path::from("foo")).copied(), Some(0));
    let apps = vec![
        (vec!["/app/test/app0/v0", "/app/test/app0/v1"], "127.0.0.1:100"),
        (vec!["/app/test/app0/v0", "/app/test/app0/v1"], "127.0.0.1:101"),
        (
            vec!["/app/test/app1/v2", "/app/test/app1/v3", "/app/test/app1/v4"],
            "127.0.0.1:105",
        ),
    ];
    let mut store = Store::new(None, BTreeMap::new());
    for (paths, addr) in &apps {
        let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
        let addr = addr.parse::<SocketAddr>().unwrap();
        let publisher = Arc::new(Publisher {
            id: PublisherId::new(),
            addr,
            hash_method: HashMethod::Sha3_512,
            resolver: addr,
            target_auth: TargetAuth::Anonymous,
        });
        publishers.insert(addr, publisher.clone());
        for path in parsed.clone() {
            store.publish(path.clone(), &publisher, false, None);
            if !store
                .resolve(&mut HashMap::default(), &path)
                .1
                .contains(&PublisherRef { id: publisher.id, token: Bytes::new() })
            {
                panic!()
            }
            if rand::thread_rng().gen_bool(0.5) {
                // check that this is idempotent
                store.publish(path.clone(), &publisher, false, None);
            }
        }
    }
    let paths = store.list(&Path::from("/"));
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].as_ref(), "/app");
    let cols = store.columns(&Path::from("/"));
    assert_eq!(cols.len(), 0);
    let paths = store.list(&Path::from("/app"));
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].as_ref(), "/app/test");
    let cols = store.columns(&Path::from("/app"));
    assert_eq!(cols.len(), 0);
    let paths = store.list(&Path::from("/app/test"));
    assert_eq!(paths.len(), 2);
    assert_eq!(paths[0].as_ref(), "/app/test/app0");
    assert_eq!(paths[1].as_ref(), "/app/test/app1");
    let mut cols = store.columns(&Path::from("/app/test"));
    cols.sort();
    assert_eq!(cols.len(), 5);
    assert_eq!(cols[0].0.as_ref(), "v0");
    assert_eq!(cols[0].1, Z64(2));
    assert_eq!(cols[1].0.as_ref(), "v1");
    assert_eq!(cols[1].1, Z64(2));
    assert_eq!(cols[2].0.as_ref(), "v2");
    assert_eq!(cols[2].1, Z64(1));
    assert_eq!(cols[3].0.as_ref(), "v3");
    assert_eq!(cols[3].1, Z64(1));
    assert_eq!(cols[4].0.as_ref(), "v4");
    assert_eq!(cols[4].1, Z64(1));
    let paths = store.list(&Path::from("/app/test/app0"));
    assert_eq!(paths.len(), 2);
    assert_eq!(paths[0].as_ref(), "/app/test/app0/v0");
    assert_eq!(paths[1].as_ref(), "/app/test/app0/v1");
    let cols = store.columns(&Path::from("/app/test/app0"));
    assert_eq!(cols.len(), 0);
    let paths = store.list(&Path::from("/app/test/app1"));
    assert_eq!(paths.len(), 3);
    assert_eq!(paths[0].as_ref(), "/app/test/app1/v2");
    assert_eq!(paths[1].as_ref(), "/app/test/app1/v3");
    assert_eq!(paths[2].as_ref(), "/app/test/app1/v4");
    let cols = store.columns(&Path::from("/app/test/app1"));
    assert_eq!(cols.len(), 0);
    let paths = store.list(&Path::from("/app/test/"));
    assert_eq!(paths.len(), 2);
    assert_eq!(paths[0].as_ref(), "/app/test/app0");
    assert_eq!(paths[1].as_ref(), "/app/test/app1");
    let (ref paths, ref addr) = apps[2];
    let addr = addr.parse::<SocketAddr>().unwrap();
    let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
    for path in parsed.clone() {
        let publisher = &publishers[&addr];
        store.unpublish(publisher, path.clone());
        if store
            .resolve(&mut HashMap::default(), &path)
            .1
            .contains(&PublisherRef { id: publisher.id, token: Bytes::new() })
        {
            panic!()
        }
        if rand::thread_rng().gen_bool(0.5) {
            // check that this is idempotent
            store.unpublish(&publisher, path.clone());
        }
    }
    let paths = store.list(&Path::from("/"));
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].as_ref(), "/app");
    let cols = store.columns(&Path::from("/"));
    assert_eq!(cols.len(), 0);
    let paths = store.list(&Path::from("/app"));
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].as_ref(), "/app/test");
    let cols = store.columns(&Path::from("/app"));
    assert_eq!(cols.len(), 0);
    let paths = store.list(&Path::from("/app/test"));
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].as_ref(), "/app/test/app0");
    let mut cols = store.columns(&Path::from("/app/test"));
    cols.sort();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].0.as_ref(), "v0");
    assert_eq!(cols[0].1, Z64(2));
    assert_eq!(cols[1].0.as_ref(), "v1");
    assert_eq!(cols[1].1, Z64(2));
    let paths = store.list(&Path::from("/app/test/app1"));
    assert_eq!(paths.len(), 0);
    let cols = store.columns(&Path::from("/app/test/app1"));
    assert_eq!(cols.len(), 0);
    let paths = store.list(&Path::from("/app/test/app0"));
    assert_eq!(paths.len(), 2);
    assert_eq!(paths[0].as_ref(), "/app/test/app0/v0");
    assert_eq!(paths[1].as_ref(), "/app/test/app0/v1");
    let cols = store.columns(&Path::from("/app/test/app0"));
    assert_eq!(cols.len(), 0);
    for (paths, addr) in &apps {
        let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
        let addr = addr.parse::<SocketAddr>().unwrap();
        let publisher = &publishers[&addr];
        for path in parsed.clone() {
            store.unpublish(publisher, path.clone());
            if store
                .resolve(&mut HashMap::default(), &path)
                .1
                .contains(&PublisherRef { id: publisher.id, token: Bytes::new() })
            {
                panic!()
            }
        }
    }
    let paths = store.list(&Path::from("/"));
    assert_eq!(paths.len(), 0);
    let cols = store.columns(&Path::from("/app/test"));
    assert_eq!(cols.len(), 0);
}

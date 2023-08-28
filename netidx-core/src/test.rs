use crate::pack::{self, Pack};
use bytes::Buf;
use chrono::prelude::*;
use rand::{thread_rng, Rng};

#[test]
fn test_naive_date_pack() {
    let mut buf = [0u8; 4];
    for d in NaiveDate::MIN.iter_days() {
        Pack::encode(&d, &mut &mut buf[..]).unwrap();
        let u = Pack::decode(&mut &buf[..]).unwrap();
        assert_eq!(d, u)
    }
}

fn check_encode_decode_short(buf: &mut [u8; 7], d: u64) {
    let mut b = &mut buf[..];
    pack::encode_varint(d, &mut b);
    let mut b = &buf[..];
    let u = pack::decode_varint(&mut b).unwrap();
    assert_eq!(7 - pack::varint_len(d), b.remaining());
    if d != u {
        panic!("{:?} {:x} != {:x}", buf, d, u)
    }
}

fn check_encode_decode(buf: &mut [u8; 16], d: u64) {
    let mut b = &mut buf[..];
    pack::encode_varint(d, &mut b);
    let mut b = &buf[..];
    let u = pack::decode_varint(&mut b).unwrap();
    assert_eq!(16 - pack::varint_len(d), b.remaining());
    if d != u {
        panic!("{:?} {:x} != {:x}", buf, d, u)
    }
}

#[test]
fn test_varint_pack_sp() {
    let mut buf = [0u8; 16];
    check_encode_decode(&mut buf, 256)
}

#[test]
fn test_varint_pack_short() {
    let mut buf = [0u8; 7];
    for d in 0..u32::MAX as u64 {
        check_encode_decode_short(&mut buf, d)
    }
}

#[test]
fn test_varint_pack() {
    let mut buf = [0u8; 16];
    let mut rng = thread_rng();
    for d in 0..1000000000 {
        check_encode_decode(&mut buf, d)
    }
    for _ in 0..1000000000 {
        let d = rng.gen::<u64>();
        check_encode_decode(&mut buf, d)
    }
}

#[test]
fn test_array_pack() {
    let mut buf = [0u8; 65];
    let a = [42u8; 64];
    Pack::encode(&a, &mut &mut buf[..]).unwrap();
    assert_eq!(<[u8; 64] as Pack>::decode(&mut &buf[..]).unwrap(), a)
}

use crate::pack::{self, Pack};
use bytes::Buf;
use chrono::prelude::*;
use rand::{RngExt, rng};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

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
    let mut rng = rng();
    for d in 0..1000000000 {
        check_encode_decode(&mut buf, d)
    }
    for _ in 0..1000000000 {
        let d = rng.random::<u64>();
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

fn pack<T: Pack>(t: &T) -> Vec<u8> {
    let mut buf = vec![0u8; t.encoded_len()];
    t.encode(&mut &mut buf[..]).unwrap();
    buf
}

fn addrs() -> Vec<SocketAddr> {
    vec![
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), u16::MAX),
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 5000),
        SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0xdead, 0xbeef, 1, 2, 3, 4)),
            u16::MAX,
        ),
    ]
}

#[test]
fn test_addr_pack_roundtrip() {
    for a in addrs() {
        let buf = pack(&a);
        assert_eq!(buf.len(), a.encoded_len());
        let mut b = &buf[..];
        assert_eq!(<SocketAddr as Pack>::decode(&mut b).unwrap(), a);
        assert_eq!(b.remaining(), 0);
        let ip = a.ip();
        let buf = pack(&ip);
        assert_eq!(buf.len(), ip.encoded_len());
        let mut b = &buf[..];
        assert_eq!(<IpAddr as Pack>::decode(&mut b).unwrap(), ip);
        assert_eq!(b.remaining(), 0);
    }
}

#[test]
fn test_addr_pack_starts_with_its_ip() {
    for a in addrs() {
        let sa = pack(&a);
        let ip = pack(&a.ip());
        assert_eq!(&sa[..ip.len()], &ip[..]);
        assert_eq!(<IpAddr as Pack>::decode(&mut &sa[..]).unwrap(), a.ip());
    }
}

#[test]
fn test_addr_pack_wire_format() {
    let v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000);
    assert_eq!(&pack(&v4)[..], &[0, 127, 0, 0, 1, 0x13, 0x88]);
    assert_eq!(&pack(&v4.ip())[..], &[0, 127, 0, 0, 1]);
    let v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 5000);
    assert_eq!(
        &pack(&v6)[..],
        &[
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x13, 0x88, 0, 0, 0, 0, 0,
            0, 0, 0
        ]
    );
    assert_eq!(&pack(&v6.ip())[..], &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
}

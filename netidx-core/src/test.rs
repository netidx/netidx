use chrono::prelude::*;
use crate::pack::Pack;

#[test]
fn test_naive_date_pack() {
    let mut buf = [0u8; 4];
    for d in NaiveDate::MIN.iter_days() {
	Pack::encode(&d, &mut &mut buf[..]).unwrap();
	let u = Pack::decode(&mut &buf[..]).unwrap();
	assert_eq!(d, u)
    }
}

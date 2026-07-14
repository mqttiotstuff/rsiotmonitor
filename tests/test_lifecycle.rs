#![allow(dead_code)]

struct A {}

impl A {
    fn iter(&self) -> ItA<'_> {
        ItA { refa: self }
    }
}

struct ItA<'a> {
    refa: &'a A,
}

#[test]
fn test_lifecycle_iterator() {
    let a = A {};
    let _it = a.iter();
}

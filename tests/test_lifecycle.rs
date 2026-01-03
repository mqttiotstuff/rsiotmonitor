



struct A {

}

impl <'a> A {
    pub fn iter(&'a self) -> ItA<'a> {
        ItA {
            refa: self,
        }
    } 
}

struct ItA<'a> {
    refa: &'a A,
}

#[test]
pub fn test() {




}


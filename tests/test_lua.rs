use mlua::{prelude::*, Variadic};

#[test]
fn test_lua_using_rust_function() -> LuaResult<()> {
    let lua = Lua::new();
    // You can also accept runtime variadic arguments to rust callbacks.

    let check_equal = lua.create_function(|_, (list1, list2): (Vec<String>, Vec<String>)| {
        // This function just checks whether two string lists are equal, and in an inefficient way.
        // Lua callbacks return `mlua::Result`, an Ok value is a normal return, and an Err return
        // turns into a Lua 'error'. Again, any type that is convertible to Lua may be returned.
        Ok(list1 == list2)
    })?;
    lua.globals().set("check_equal", check_equal)?;

    let join = lua.create_function(|_, strings: Variadic<String>| {
        // (This is quadratic!, it's just an example!)
        Ok(strings.iter().fold("".to_owned(), |a, b| a + b))
    })?;
    lua.globals().set("join", join)?;

    assert!(lua
        .load(r#"check_equal({"a", "b", "c"}, {"a", "b", "c"})"#)
        .eval::<bool>()?);
    assert!(!lua
        .load(r#"check_equal({"a", "b", "c"}, {"d", "e", "f"})"#)
        .eval::<bool>()?);
    assert_eq!(lua.load(r#"join("a", "b", "c")"#).eval::<String>()?, "abc");
    Ok(())
}

#[test]
fn test_lua_table() -> LuaResult<()> {
    let lua = Lua::new();

    let map_table = lua.create_table()?;
    map_table.set(1, "one")?;
    map_table.set("two", 2)?;

    lua.globals().set("map_table", map_table)?;

    lua.load("for k,v in pairs(map_table) do print(k,v) end")
        .exec()?;

    Ok(())
}

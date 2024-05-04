///! Evaluating the lua embedded language
use std::error::Error;

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

#[test]
fn test_lua_agent() -> LuaResult<()> {
    let lua = Lua::new();

    let map_table = lua.create_table()?;
    map_table.set(1, "one")?;
    map_table.set("two", 2)?;

    lua.globals().set("map_table", map_table)?;

    let lua_code = r#"

        -- Generic class
        Agent = {}
        function Agent:new ()
            o = {}
            self.register = function(service)
                print(self)
                print(service)

            end

            setmetatable(o, self)
            self.__index = self
            return o
        end


        local player = Agent:new()
        player.register = function(service) 
            print("overloaded")
            print(self)
            print(service)
            return { register= "toto" }
        end

        player:register({})
       
        return player
    "#;

    let lua_code = lua.load(lua_code);
    let res: mlua::Table = lua_code.eval()?;

    println!("result of function : {:?}", res);

    // call the register function
    let result_code: mlua::Table = res.call_function("register", lua.create_table())?;
    println!("result of register call : {:?}", result_code);

    Ok(())
}

// some rust function definition, participating to module
fn sum(_: &Lua, (a, b): (i64, i64)) -> LuaResult<i64> {
    Ok(a + b)
}

// some rust function definition, participating to module
fn used_memory(lua: &Lua, _: ()) -> LuaResult<usize> {
    Ok(lua.used_memory())
}

// create rust module passed to lua execution context
fn rust_module(lua: &Lua) -> LuaResult<LuaTable> {
    let exports = lua.create_table()?;
    exports.set("sum", lua.create_function(sum)?)?;
    exports.set("used_memory", lua.create_function(used_memory)?)?;
    Ok(exports)
}

#[test]
fn test_lua_passed_function() -> std::result::Result<(), Box<dyn Error>> {
    // this element test inline module passing to lua as parameter in function

    let lua = Lua::new();
    let compiled = lua.load(
        r#"
        function(m) 
            -- module passed 
            return m.sum(1,5) -- call function
        end
    "#,
    );

    let module = rust_module(&lua)?;

    let f: LuaFunction = compiled.eval().unwrap();

    let r: i32 = f.call(module)?;

    println!("result : {:?}", r);

    Ok(())
}

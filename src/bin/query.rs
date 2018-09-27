extern crate diesel;
extern crate jsonld;
extern crate kroeg_cellar;
extern crate serde_json;

use diesel::result::Error;
use diesel::{pg::PgConnection, Connection};
use jsonld::{
    nodemap::DefaultNodeGenerator,
    rdf::{jsonld_to_rdf, rdf_to_jsonld},
};
use kroeg_cellar::QuadClient;
use serde_json::{from_reader, Value};
use std::collections::HashMap;
use std::env;
use std::io::{stdin, Read};

fn help(val: &str) -> Result<(), Error> {
    eprintln!("Usage: {} [database url] get/set [id]", val);
    eprintln!(" - get returns a JSON-LD expanded object on stdout");
    eprintln!(" - set expects one on stdin");

    Ok(())
}

fn get(client: &QuadClient, id: &str) -> Result<(), Error> {
    let quads = client.read_quads(id)?;
    if quads.len() == 0 {
        println!("{{}}");
        return Ok(());
    }

    let mut hash = HashMap::new();
    hash.insert("@default".to_owned(), quads);

    let json = if let Value::Object(obj) = rdf_to_jsonld(hash, true, false) {
        Value::Array(obj.into_iter().map(|(a, b)| b).collect())
    } else {
        Value::Null
    };

    println!("{}", json.to_string());

    Ok(())
}

fn set(client: &mut QuadClient, id: &str) -> Result<(), Error> {
    let json = from_reader(stdin()).unwrap();
    let quads = jsonld_to_rdf(json, &mut DefaultNodeGenerator::new())
        .unwrap()
        .remove("@default")
        .unwrap();

    client.write_quads(id, quads)?;

    Ok(())
}

fn main() -> Result<(), Error> {
    let args: Vec<_> = env::args().collect();

    if args.len() != 4 {
        return help(&args[0]);
    }

    let conn = PgConnection::establish(&args[1]).unwrap();
    let mut client = QuadClient::new(conn);

    match &args[2] as &str {
        "get" => get(&client, &args[3]),
        "set" => set(&mut client, &args[3]),
        _ => help(&args[0]),
    }
}

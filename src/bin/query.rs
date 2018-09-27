extern crate diesel;
extern crate futures;
extern crate jsonld;
extern crate kroeg_cellar;
extern crate kroeg_tap;
extern crate serde_json;

use diesel::result::Error;
use diesel::{pg::PgConnection, Connection};
use futures::Future;
use jsonld::{
    nodemap::DefaultNodeGenerator,
    rdf::{jsonld_to_rdf, rdf_to_jsonld},
};
use kroeg_cellar::QuadClient;
use kroeg_tap::EntityStore;
use serde_json::{from_reader, Value};
use std::collections::HashMap;
use std::env;
use std::io::stdin;

fn help(val: &str) -> Result<(), Error> {
    eprintln!("Usage: {} [database url] get/set [id]", val);
    eprintln!(" - get returns a JSON-LD expanded object on stdout");
    eprintln!(" - set expects one on stdin");
    eprintln!(
        "-- Collections: {} [database url] collection insert/remove <collection id> <id>",
        val
    );
    eprintln!("Insert/remove arbitrary IDs into arbitrary collections.");
    eprintln!(
        "Also: {} [database url] collection list <collection id>",
        val
    );
    eprintln!("Get all the IDs in a collection.");

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
        Value::Array(obj.into_iter().map(|(_, b)| b).collect())
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

fn collection_insert(client: &mut QuadClient, id: &str, object: &str) -> Result<(), Error> {
    client
        .insert_collection(id.to_owned(), object.to_owned())
        .poll()?;

    Ok(())
}

fn collection_list(client: &mut QuadClient, id: &str) -> Result<(), Error> {
    if let futures::Async::Ready(data) = client
        .read_collection(id.to_owned(), Some(u32::max_value()), None)
        .poll()?
    {
        for item in data.items {
            println!("{}", item);
        }
    }

    Ok(())
}

fn collection_remove(client: &mut QuadClient, id: &str, object: &str) -> Result<(), Error> {
    client
        .remove_collection(id.to_owned(), object.to_owned())
        .poll()?;

    Ok(())
}

fn main() -> Result<(), Error> {
    let args: Vec<_> = env::args().collect();

    // query [connection] [first] [second]
    if args.len() < 4 || args.len() > 6 {
        return help(&args[0]);
    }

    let conn = PgConnection::establish(&args[1]).unwrap();
    let mut client = QuadClient::new(conn);

    match (
        &args[2] as &str,
        &args[3] as &str,
        args.get(4).map(|f| f as &str),
        args.get(5),
    ) {
        ("get", id, None, None) => get(&client, id),
        ("set", id, None, None) => set(&mut client, id),
        ("collection", "list", Some(collection), None) => collection_list(&mut client, collection),
        ("collection", "insert", Some(collection), Some(object)) => {
            collection_insert(&mut client, collection, object)
        }
        ("collection", "remove", Some(collection), Some(object)) => {
            collection_remove(&mut client, collection, object)
        }
        _ => help(&args[0]),
    }
}

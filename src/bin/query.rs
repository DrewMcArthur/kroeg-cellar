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

fn help(val: &str) -> Result<(), (Error, QuadClient)> {
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

fn get(mut client: QuadClient, id: &str) -> Result<QuadClient, (Error, QuadClient)> {
    let quads = match client.read_quads(id) {
        Ok(ok) => ok,
        Err(e) => return Err((e, client)),
    };

    if quads.len() == 0 {
        println!("{{}}");
        return Ok(client);
    }

    let mut hash = HashMap::new();
    hash.insert("@default".to_owned(), quads);

    let json = if let Value::Object(obj) = rdf_to_jsonld(hash, true, false) {
        Value::Array(obj.into_iter().map(|(_, b)| b).collect())
    } else {
        Value::Null
    };

    println!("{}", json.to_string());

    Ok(client)
}

fn set(mut client: QuadClient, id: &str) -> Result<QuadClient, (Error, QuadClient)> {
    let json = from_reader(stdin()).unwrap();
    let quads = jsonld_to_rdf(json, &mut DefaultNodeGenerator::new())
        .unwrap()
        .remove("@default")
        .unwrap();

    if let Err(e) = client.write_quads(id, quads) {
        Err((e, client))
    } else {
        Ok(client)
    }
}

fn collection_insert(client: QuadClient, id: &str, object: &str) -> Result<QuadClient, (Error, QuadClient)> {
    if let futures::Async::Ready(data) = client
        .insert_collection(id.to_owned(), object.to_owned())
        .poll()? {
        Ok(data)
    } else {
        unreachable!();
    }
}

fn collection_list(client: QuadClient, id: &str) -> Result<QuadClient, (Error, QuadClient)> {
    if let futures::Async::Ready((data, client)) = client
        .read_collection(id.to_owned(), Some(u32::max_value()), None)
        .poll()?
    {
        for item in data.items {
            println!("{}", item);
        }

        return Ok(client);
    }

    unreachable!();
}

fn collection_remove(client: QuadClient, id: &str, object: &str) -> Result<QuadClient, (Error, QuadClient)> {
    if let futures::Async::Ready(data) = client
        .remove_collection(id.to_owned(), object.to_owned())
        .poll()? {
        Ok(data)
    } else {
        unreachable!();
    }
}

fn main() -> Result<(), (Error, QuadClient)> {
    let args: Vec<_> = env::args().collect();

    // query [connection] [first] [second]
    if args.len() < 4 || args.len() > 6 {
        return help(&args[0]);
    }

    let conn = PgConnection::establish(&args[1]).unwrap();
    let client = QuadClient::new(conn);

    match (
        &args[2] as &str,
        &args[3] as &str,
        args.get(4).map(|f| f as &str),
        args.get(5),
    ) {
        ("get", id, None, None) => get(client, id),
        ("set", id, None, None) => set(client, id),
        ("collection", "list", Some(collection), None) => collection_list(client, collection),
        ("collection", "insert", Some(collection), Some(object)) => {
            collection_insert(client, collection, object)
        }
        ("collection", "remove", Some(collection), Some(object)) => {
            collection_remove(client, collection, object)
        }
        _ => return help(&args[0]),
    }.map(|_| ())
}

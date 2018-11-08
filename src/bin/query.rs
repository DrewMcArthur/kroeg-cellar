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
    rdf::{jsonld_to_rdf, rdf_to_jsonld, QuadContents},
};
use kroeg_cellar::QuadClient;
use kroeg_tap::EntityStore;
use serde_json::{from_reader, Value};
use std::collections::HashMap;
use std::env;
use std::io::stdin;
use std::io::{self, BufRead};

fn help(val: &str) -> Result<(), (Error, QuadClient)> {
    eprintln!(
        "Retrieve single objects: {} [database url] (get|set) [id]",
        val
    );
    eprintln!(" - get returns a JSON-LD expanded object on stdout");
    eprintln!(" - set expects one on stdin");
    eprintln!(
        "Write collections: {} [database url] collection insert/remove <collection id> <id>",
        val
    );
    eprintln!(
        "Read collections: {} [database url] collection list <collection id>",
        val
    );

    eprintln!("[WARNING: DANGEROUS] {} [database url] migrate [id]", val);
    eprintln!("  Migrates an object from quad to triple format.");

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

fn migrate(mut client: QuadClient, id: &str) -> Result<QuadClient, (Error, QuadClient)> {
    if id == "all" {
        return migrate_all(client);
    }

    let quads = match client.read_quads(id) {
        Ok(ok) => ok,
        Err(e) => return Err((e, client)),
    };

    let mut triples = HashMap::new();

    for mut quad in quads {
        if quad.subject_id.starts_with("_:") {
            quad.subject_id = format!("_:{}{}", id, &quad.subject_id[1..]);
        }

        let id_val = if quad.subject_id == "https://puckipedia.com/kroeg/ns#meta" {
            id.to_owned()
        } else {
            quad.subject_id.to_owned()
        };

        if !triples.contains_key(&id_val) {
            triples.insert(id_val.to_owned(), Vec::new());
        }

        match quad.contents {
            QuadContents::Id(ref mut val) => {
                if val.starts_with("_:") {
                    *val = format!("_:{}{}", id, &val[1..]);
                }
            }

            _ => {}
        }

        triples.get_mut(&id_val).unwrap().push(quad);
    }

    for (k, v) in triples {
        println!("Storing {}", k);

        match client.write_quads(&k, v) {
            Ok(_) => {}
            Err(e) => return Err((e, client)),
        }
    }

    Ok(client)
}

fn migrate_all(mut client: QuadClient) -> Result<QuadClient, (Error, QuadClient)> {
    match client.preload_all() {
        Ok(items) => println!("LOADED {} ATTRIBUTE IDS", items),
        Err(e) => return Err((e, client)),
    }

    println!("Will update *ALL* quads. ARE YOU SURE??? Press enter to continue.");

    let mut line = String::new();
    let stdin = io::stdin();
    let _ = stdin.lock().read_line(&mut line).unwrap();

    let mut previous_id = 0;
    let mut i = 0;

    client.begin_transaction();
    loop {
        let (items, id) = match client.get_quads(previous_id) {
            Ok(val) => val,
            Err(e) => return Err((e, client)),
        };

        if items.len() == 0 {
            println!("Done! Processed {} items", i);
            client.commit_transaction();
            return Ok(client);
        }

        for item in items {
            i += 1;
            println!("Item {}: {}", i, item);
            client = migrate(client, &item)?;
        }

        previous_id = id;
    }
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

fn collection_insert(
    client: QuadClient,
    id: &str,
    object: &str,
) -> Result<QuadClient, (Error, QuadClient)> {
    if let futures::Async::Ready(data) = client
        .insert_collection(id.to_owned(), object.to_owned())
        .poll()?
    {
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

fn collection_remove(
    client: QuadClient,
    id: &str,
    object: &str,
) -> Result<QuadClient, (Error, QuadClient)> {
    if let futures::Async::Ready(data) = client
        .remove_collection(id.to_owned(), object.to_owned())
        .poll()?
    {
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
        ("migrate", id, None, None) => migrate(client, id),
        _ => return help(&args[0]),
    }
    .map(|_| ())
}

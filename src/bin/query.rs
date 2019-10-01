use kroeg_cellar::{CellarConnection, CellarEntityStore};
use kroeg_tap::{EntityStore, StoreError, StoreItem};
use serde_json::{from_reader, Value};
use std::env;
use std::time::Instant;

async fn help(val: &str) -> Result<(), StoreError> {
    eprintln!(
        "To use: {} <address:port> <username> <password> <database> [command]",
        val
    );
    eprintln!("Retrieve single objects: (get|set) <id>");
    eprintln!(" - get returns a JSON-LD expanded object on stdout");
    eprintln!(" - set expects one on stdin");
    eprintln!("Write collections: collection (insert|delete) <collection id> <id>");
    eprintln!("Read collections: collection list <collection id>");

    Ok(())
}

async fn get(client: &mut CellarEntityStore<'_>, id: &str) -> Result<(), StoreError> {
    let data = client.get((*id).to_owned(), false).await?;
    if let Some(data) = data {
        println!("{}", data.to_json().to_string());
    } else {
        println!("{{}}");
    }

    Ok(())
}

async fn set(client: &mut CellarEntityStore<'_>, id: &str) -> Result<(), StoreError> {
    let mut json: Value = from_reader(std::io::stdin()).unwrap();
    if !json.is_array() {
        json = Value::Array(vec![json]);
    }

    let mut store_item = StoreItem::parse(id, &json)?;

    client.put(id.to_owned(), &mut store_item).await?;

    Ok(())
}

async fn collection_insert(
    client: &mut CellarEntityStore<'_>,
    id: &str,
    object: &str,
) -> Result<(), StoreError> {
    EntityStore::insert_collection(client, id.to_owned(), object.to_owned()).await
}

async fn collection_list(client: &mut CellarEntityStore<'_>, id: &str) -> Result<(), StoreError> {
    let data = client
        .read_collection(id.to_owned(), Some(u32::max_value()), None)
        .await?;

    for item in data.items {
        println!("{}", item);
    }

    Ok(())
}

async fn collection_remove(
    client: &mut CellarEntityStore<'_>,
    id: &str,
    object: &str,
) -> Result<(), StoreError> {
    client
        .remove_collection(id.to_owned(), object.to_owned())
        .await
}

async fn run_code() -> Result<(), StoreError> {
    let args: Vec<_> = env::args().collect();

    if args.len() < 6 {
        return help(&args[0]).await;
    }

    let conn = CellarConnection::connect(&args[1], &args[2], &args[3], &args[4]).await?;
    let mut session = CellarEntityStore::new(&conn);

    eprintln!("ready.\n");

    let args: Vec<_> = args.iter().skip(5).map(String::as_ref).collect();

    let start = Instant::now();
    let result = match &args as &[&str] {
        ["get", id] => get(&mut session, id).await,
        ["set", id] => set(&mut session, id).await,
        ["collection", "insert", id, object] => collection_insert(&mut session, id, object).await,
        ["collection", "delete", id, object] => collection_remove(&mut session, id, object).await,
        ["collection", "list", id] => collection_list(&mut session, id).await,
        _ => help(&args[0]).await,
    };

    eprintln!("Took {:?}", Instant::now() - start);

    result
}

fn main() -> Result<(), StoreError> {
    async_std::task::block_on(run_code())
}

use std::collections::HashSet;

use postgres_async::types::Row;

/// The contents of a single database quad.
pub enum DatabaseQuadContents {
    /// This quad points to another subject.
    Id(i32),

    /// This quad consists of a type id, and the contents (serialized as string, like RDF)
    Object { contents: String, type_id: i32 },

    /// This quad consists of a string, and an associated language.
    LanguageString { contents: String, language: String },
}

/// The raw contents of a single database quad.
pub struct DatabaseQuad {
    pub id: i32,
    pub quad_id: i32,
    pub subject_id: i32,
    pub predicate_id: i32,
    pub contents: DatabaseQuadContents,
}

impl DatabaseQuad {
    pub fn make_from_row(row: &Row) -> DatabaseQuad {
        let contents = match (
            row.get(4).unwrap(),
            row.get(5).unwrap(),
            row.get(6).unwrap(),
            row.get(7).unwrap(),
        ) {
            (Some(id), _, _, _) => DatabaseQuadContents::Id(id),
            (_, Some(contents), _, Some(language)) => DatabaseQuadContents::LanguageString {
                contents: contents,
                language: language,
            },
            (_, Some(contents), Some(type_id), _) => DatabaseQuadContents::Object {
                contents: contents,
                type_id: type_id,
            },
            _ => panic!("invalid quad contents; impossible"),
        };

        DatabaseQuad {
            id: row.get(0).unwrap().unwrap(),
            quad_id: row.get(1).unwrap().unwrap(),
            subject_id: row.get(2).unwrap().unwrap(),
            predicate_id: row.get(3).unwrap().unwrap(),
            contents: contents,
        }
    }
}

/// Collects all IDs used inside the passed quads. Can be used to cache all the IDs.
pub fn collect_quad_ids(quads: &[DatabaseQuad]) -> HashSet<i32> {
    let mut out = HashSet::new();

    for quad in quads {
        out.insert(quad.quad_id);
        out.insert(quad.subject_id);
        out.insert(quad.predicate_id);
        match quad.contents {
            DatabaseQuadContents::Id(id) => out.insert(id),
            DatabaseQuadContents::Object { type_id: id, .. } => out.insert(id),
            _ => false,
        };
    }

    out
}

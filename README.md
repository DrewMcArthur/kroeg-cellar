# Kroeg Cellar

(I'd have called this `kroeg-kelder` but that'd probably go too far)

Simple JSON-LD storage mechanism, storing everything in a database of RDF
 quads.

## Usage

This code is made mostly to work inside of Kroeg, but the external API is
 documented with rustdoc, which isn't publicly generated yet. Clone therepo
 then run `cargo doc` to do it yourself.

## Design

The database currently stores its data in two tables:

### `Attribute`

This table maps an ID number to a URL, allowing for e.g. quick lookups and even
 quicker renaming of objects (just change its value in this table, and
 everything automatically updates!)

### `Quad`
This table is slightly complicated, as it has to support everything that RDF
 can do:

- `id`: unique ID for the quad.
- `quad_id`, `subject_id`, `predicate_id`: These point into the `Attribute`
   table, for the corresponding RDF quad attributes.

- `attribute_id`: used if object points to another ID.

- `object`, `type_id`, `language`: The value, and type or language of the quad.
  The code only supports `language` or `type_id` being set, both of them being
   set is unsupported.

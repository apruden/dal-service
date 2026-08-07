use std::collections::BTreeMap;

use flatbuffers::{Follow, ForwardsUOffset, Table, VOffsetT, Vector, Verifiable, WIPOffset};
use flexbuffers::{FlexBufferType, Reader};

use crate::error::{Error, Result};
use crate::search::{FieldKind, PathSegment, SearchIndexDefinition};

const IDENTIFIER: &str = "DALV";

/// Minimal generated-equivalent binding for `schema/search_value.fbs`.
#[derive(Clone, Copy)]
struct SearchValueEnvelope<'a> {
    table: Table<'a>,
}

impl<'a> Follow<'a> for SearchValueEnvelope<'a> {
    type Inner = Self;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            // SAFETY: `flatbuffers::root` verifies this location before Follow.
            table: unsafe { Table::new(buf, loc) },
        }
    }
}

impl SearchValueEnvelope<'_> {
    const VT_TYPE_ID: VOffsetT = 4;
    const VT_PAYLOAD: VOffsetT = 6;

    fn type_id(&self) -> &str {
        // SAFETY: required field and string shape are checked by Verifiable.
        unsafe {
            self.table
                .get::<ForwardsUOffset<&str>>(Self::VT_TYPE_ID, None)
                .expect("verified required type_id")
        }
    }

    fn payload(&self) -> Vector<'_, u8> {
        // SAFETY: required field and vector shape are checked by Verifiable.
        unsafe {
            self.table
                .get::<ForwardsUOffset<Vector<'_, u8>>>(Self::VT_PAYLOAD, None)
                .expect("verified required payload")
        }
    }
}

impl Verifiable for SearchValueEnvelope<'_> {
    fn run_verifier(
        verifier: &mut flatbuffers::Verifier,
        position: usize,
    ) -> std::result::Result<(), flatbuffers::InvalidFlatbuffer> {
        verifier
            .visit_table(position)?
            .visit_field::<ForwardsUOffset<&str>>("type_id", Self::VT_TYPE_ID, true)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>("payload", Self::VT_PAYLOAD, true)?
            .finish();
        Ok(())
    }
}

/// Build the canonical FlatBuffer envelope used by search-aware DAL values.
pub fn encode_search_value(type_id: &str, flex_payload: &[u8]) -> Result<Vec<u8>> {
    if type_id.is_empty() {
        return Err(Error::Search("value type_id must not be empty".into()));
    }
    Reader::get_root(flex_payload)
        .map_err(|error| Error::Search(format!("invalid FlexBuffer payload: {error}")))?;

    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let type_id = builder.create_string(type_id);
    let payload = builder.create_vector(flex_payload);
    let start = builder.start_table();
    builder.push_slot_always::<WIPOffset<_>>(SearchValueEnvelope::VT_TYPE_ID, type_id);
    builder.push_slot_always::<WIPOffset<_>>(SearchValueEnvelope::VT_PAYLOAD, payload);
    let table = builder.end_table(start);
    builder.required(table, SearchValueEnvelope::VT_TYPE_ID, "type_id");
    builder.required(table, SearchValueEnvelope::VT_PAYLOAD, "payload");
    let root = WIPOffset::<SearchValueEnvelope<'_>>::new(table.value());
    builder.finish(root, Some(IDENTIFIER));
    Ok(builder.finished_data().to_vec())
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExtractedValue {
    Text(String),
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    Date(i64),
    Facet(String),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExtractedDocument {
    pub fields: BTreeMap<String, Vec<ExtractedValue>>,
}

/// Verify an envelope and deterministically extract fields for one definition.
/// A valid value of a different type is outside this index and returns `None`.
pub fn extract_document(
    encoded: &[u8],
    definition: &SearchIndexDefinition,
) -> Result<Option<ExtractedDocument>> {
    definition.validate()?;
    if !flatbuffers::buffer_has_identifier(encoded, IDENTIFIER, false) {
        return Err(Error::Search(
            "value is not a DALV SearchValue FlatBuffer".into(),
        ));
    }
    let envelope = flatbuffers::root::<SearchValueEnvelope<'_>>(encoded)
        .map_err(|error| Error::Search(format!("invalid SearchValue FlatBuffer: {error}")))?;
    if envelope.type_id() != definition.document_type {
        return Ok(None);
    }

    let root = Reader::get_root(envelope.payload().bytes())
        .map_err(|error| Error::Search(format!("invalid FlexBuffer payload: {error}")))?;
    if root.flexbuffer_type() != FlexBufferType::Map {
        return Err(Error::Search("FlexBuffer root must be a map".into()));
    }

    let mut document = ExtractedDocument::default();
    let mut extracted_bytes = 0usize;
    for field in &definition.fields {
        let Some(reader) = traverse(root.clone(), &field.source_path)? else {
            if field.required {
                return Err(Error::Search(format!(
                    "required field {:?} is missing",
                    field.name
                )));
            }
            continue;
        };

        let values = if field.multi_valued {
            let vector = reader
                .get_vector()
                .map_err(|_| Error::Search(format!("field {:?} must be a vector", field.name)))?;
            if vector.len() > crate::search::SEARCH_MAX_VALUES_PER_FIELD {
                return Err(Error::Search(format!(
                    "field {:?} exceeds the value-count limit",
                    field.name
                )));
            }
            let mut values = Vec::with_capacity(vector.len());
            for item in vector.iter() {
                values.push(extract_scalar(&field.name, &field.kind, &item)?);
            }
            values
        } else {
            vec![extract_scalar(&field.name, &field.kind, &reader)?]
        };
        for value in &values {
            let bytes = match value {
                ExtractedValue::Text(value) | ExtractedValue::Facet(value) => value.len(),
                _ => std::mem::size_of::<u64>(),
            };
            if bytes > crate::search::SEARCH_MAX_FIELD_BYTES {
                return Err(Error::Search(format!(
                    "field {:?} exceeds the byte limit",
                    field.name
                )));
            }
            extracted_bytes = extracted_bytes.saturating_add(bytes);
        }
        if extracted_bytes > crate::search::SEARCH_MAX_EXTRACTED_BYTES {
            return Err(Error::Search(
                "document exceeds extracted byte limit".into(),
            ));
        }
        document.fields.insert(field.name.clone(), values);
    }
    Ok(Some(document))
}

fn traverse<'a>(
    mut reader: Reader<&'a [u8]>,
    path: &[PathSegment],
) -> Result<Option<Reader<&'a [u8]>>> {
    for segment in path {
        reader = match segment {
            PathSegment::Key(key) => {
                let map = reader
                    .get_map()
                    .map_err(|_| Error::Search(format!("path component {key:?} requires a map")))?;
                if map.index_key(key).is_none() {
                    return Ok(None);
                }
                map.index(key.as_str()).map_err(|error| {
                    Error::Search(format!("invalid map value at {key:?}: {error}"))
                })?
            }
            PathSegment::Index(index) => {
                let vector = reader.get_vector().map_err(|_| {
                    Error::Search(format!("path component [{index}] requires a vector"))
                })?;
                let index = *index as usize;
                if index >= vector.len() {
                    return Ok(None);
                }
                vector.index(index).map_err(|error| {
                    Error::Search(format!("invalid vector value at [{index}]: {error}"))
                })?
            }
        };
    }
    Ok(Some(reader))
}

fn extract_scalar(
    field_name: &str,
    kind: &FieldKind,
    reader: &Reader<&[u8]>,
) -> Result<ExtractedValue> {
    let wrong_type = || {
        Error::Search(format!(
            "field {field_name:?} has FlexBuffer type {:?}, incompatible with {kind:?}",
            reader.flexbuffer_type()
        ))
    };
    match kind {
        FieldKind::Text { .. } | FieldKind::Keyword => reader
            .get_str()
            .map(|value| ExtractedValue::Text(value.to_string()))
            .map_err(|_| wrong_type()),
        FieldKind::U64 => reader
            .get_u64()
            .map(ExtractedValue::U64)
            .map_err(|_| wrong_type()),
        FieldKind::I64 => reader
            .get_i64()
            .map(ExtractedValue::I64)
            .map_err(|_| wrong_type()),
        FieldKind::F64 => reader
            .get_f64()
            .map(ExtractedValue::F64)
            .map_err(|_| wrong_type()),
        FieldKind::Bool => reader
            .get_bool()
            .map(ExtractedValue::Bool)
            .map_err(|_| wrong_type()),
        FieldKind::Date => reader
            .get_i64()
            .map(ExtractedValue::Date)
            .map_err(|_| wrong_type()),
        FieldKind::Facet => reader
            .get_str()
            .map(|value| ExtractedValue::Facet(value.to_string()))
            .map_err(|_| wrong_type()),
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;
    use crate::search::{SearchField, SearchIndexDefinition};

    #[derive(Serialize)]
    struct Payload<'a> {
        title: &'a str,
        tags: Vec<&'a str>,
        views: u64,
    }

    fn definition() -> SearchIndexDefinition {
        SearchIndexDefinition {
            document_type: "article".into(),
            fields: vec![
                SearchField {
                    name: "title".into(),
                    source_path: vec![PathSegment::Key("title".into())],
                    kind: FieldKind::Text {
                        tokenizer: "default".into(),
                        positions: true,
                    },
                    required: true,
                    multi_valued: false,
                    indexed: true,
                    stored: true,
                    fast: false,
                },
                SearchField {
                    name: "tags".into(),
                    source_path: vec![PathSegment::Key("tags".into())],
                    kind: FieldKind::Keyword,
                    required: false,
                    multi_valued: true,
                    indexed: true,
                    stored: false,
                    fast: false,
                },
                SearchField {
                    name: "views".into(),
                    source_path: vec![PathSegment::Key("views".into())],
                    kind: FieldKind::U64,
                    required: true,
                    multi_valued: false,
                    indexed: true,
                    stored: false,
                    fast: true,
                },
            ],
            default_search_fields: vec!["title".into()],
        }
    }

    #[test]
    fn verifies_envelope_and_extracts_exact_types() {
        let payload = flexbuffers::to_vec(Payload {
            title: "correctness first",
            tags: vec!["rust", "search"],
            views: 42,
        })
        .unwrap();
        let encoded = encode_search_value("article", &payload).unwrap();
        let document = extract_document(&encoded, &definition()).unwrap().unwrap();
        assert_eq!(
            document.fields["title"],
            vec![ExtractedValue::Text("correctness first".into())]
        );
        assert_eq!(
            document.fields["tags"],
            vec![
                ExtractedValue::Text("rust".into()),
                ExtractedValue::Text("search".into())
            ]
        );
        assert_eq!(document.fields["views"], vec![ExtractedValue::U64(42)]);
    }

    #[test]
    fn different_type_is_not_a_document() {
        let payload = flexbuffers::to_vec(Payload {
            title: "ignored",
            tags: vec![],
            views: 0,
        })
        .unwrap();
        let encoded = encode_search_value("comment", &payload).unwrap();
        assert_eq!(extract_document(&encoded, &definition()).unwrap(), None);
    }

    #[test]
    fn rejects_scalar_coercion_and_unverified_envelopes() {
        #[derive(Serialize)]
        struct Wrong<'a> {
            title: &'a str,
            tags: Vec<&'a str>,
            views: &'a str,
        }
        let payload = flexbuffers::to_vec(Wrong {
            title: "bad",
            tags: vec![],
            views: "42",
        })
        .unwrap();
        let encoded = encode_search_value("article", &payload).unwrap();
        assert!(extract_document(&encoded, &definition()).is_err());
        assert!(extract_document(b"not-flatbuffers", &definition()).is_err());
    }
}

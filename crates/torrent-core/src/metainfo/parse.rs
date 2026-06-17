use bytes::Bytes;

use crate::bencode::{self, Bencode, dict_get, dict_get_bytes, dict_get_int};
use crate::error::{Error, ErrorKind};

use super::{FileInfo, Info, Metainfo, Mode, RawInfo};

/// Parse a `Metainfo` from raw bencoded bytes (the contents of a `.torrent` file).
///
/// Performs all required validation per BEP 3:
/// - Root value must be a bencoded dictionary
/// - The `announce` and `info` keys are required
/// - The `info` dict must contain `name`, `piece length`, and `pieces`
/// - Either `length` (single-file) or `files` (multi-file) must be present
/// - `pieces` must be a multiple of 20 bytes (SHA-1 hashes)
///
/// # Errors
///
/// Returns an error if the data is not valid bencode or if required
/// metainfo fields are missing or invalid.
///
/// # Examples
///
/// ```no_run
/// use torrent_core::metainfo::from_bytes;
///
/// let data = std::fs::read("debian.torrent").unwrap();
/// let meta = from_bytes(&data).unwrap();
/// println!("Info hash: {:x?}", meta.info_hash());
/// ```
pub(crate) fn from_bytes(data: &[u8]) -> Result<Metainfo, Error> {
    tracing::debug!("parsing .torrent file ({} bytes)", data.len());
    let (val, _rest) = bencode::decode(data)?;

    // Validate that the root value is a Dict
    match val {
        Bencode::Dict(_) => {}
        _ => {
            tracing::warn!("metainfo: root is not a dict");
            return Err(Error::new(ErrorKind::MetainfoInvalidField));
        }
    }

    // --- Required fields ---

    tracing::debug!("extracting announce URL");
    let announce = get_required_string(&val, b"announce")?;

    let info_val = dict_get(&val, b"info").ok_or(Error::new(ErrorKind::MetainfoMissingField))?;

    tracing::debug!("parsing info dict");
    // Save the raw bytes of the info dict for info_hash calculation.
    // We need to find the exact byte range in the original input.
    // Re-encode it to get a canonical representation.
    let info_bytes = Bytes::from(bencode::encode(info_val));

    let info = parse_info(info_val, info_bytes)?;
    tracing::debug!(
        "metainfo parsed: announce={}, pieces={}, total_size={}",
        announce,
        info.num_pieces(),
        info.total_size()
    );

    // --- Optional fields ---

    let announce_list = parse_announce_list(&val);
    let creation_date = dict_get_int(&val, b"creation date");
    let comment = dict_get(&val, b"comment").and_then(|v| string_from_bencode(v).ok());
    let created_by = dict_get(&val, b"created by").and_then(|v| string_from_bencode(v).ok());
    let encoding = dict_get(&val, b"encoding").and_then(|v| string_from_bencode(v).ok());

    Ok(Metainfo {
        announce,
        announce_list,
        info,
        creation_date,
        comment,
        created_by,
        encoding,
    })
}

/// Parse the `info` dictionary.
fn parse_info(val: &Bencode, raw_info: Bytes) -> Result<Info, Error> {
    let piece_length =
        dict_get_int(val, b"piece length").ok_or(Error::new(ErrorKind::MetainfoMissingField))?;
    if piece_length <= 0 {
        return Err(Error::new(ErrorKind::MetainfoInvalidField));
    }

    let pieces_bytes =
        dict_get_bytes(val, b"pieces").ok_or(Error::new(ErrorKind::MetainfoMissingField))?;
    if pieces_bytes.len() % 20 != 0 {
        return Err(Error::new(ErrorKind::MetainfoInvalidPieces));
    }
    let pieces: Vec<[u8; 20]> = pieces_bytes
        .chunks_exact(20)
        .map(|chunk| {
            let mut arr = [0u8; 20];
            arr.copy_from_slice(chunk);
            arr
        })
        .collect();

    let name = get_required_string(val, b"name")?;

    let mode = if let Some(length) = dict_get_int(val, b"length") {
        if length < 0 {
            return Err(Error::new(ErrorKind::MetainfoInvalidField));
        }
        Mode::Single {
            name,
            length: length as u64,
        }
    } else if let Some(files_val) = dict_get(val, b"files") {
        let files = parse_files_list(files_val)?;
        Mode::Multiple { name, files }
    } else {
        return Err(Error::new(ErrorKind::MetainfoMissingField));
    };

    Ok(Info {
        piece_length: piece_length as u64,
        pieces,
        mode,
        raw_info: RawInfo::Bytes(raw_info),
    })
}

/// Parse the `files` list in multi-file mode.
fn parse_files_list(val: &Bencode) -> Result<Vec<FileInfo>, Error> {
    let list = match val {
        Bencode::List(items) => items,
        _ => return Err(Error::new(ErrorKind::MetainfoInvalidField)),
    };

    let mut files = Vec::with_capacity(list.len());
    for item in list {
        let length =
            dict_get_int(item, b"length").ok_or(Error::new(ErrorKind::MetainfoMissingField))?;
        if length < 0 {
            return Err(Error::new(ErrorKind::MetainfoInvalidField));
        }

        let path_parts = match dict_get(item, b"path") {
            Some(Bencode::List(parts)) => parts,
            _ => return Err(Error::new(ErrorKind::MetainfoInvalidField)),
        };

        let mut path = Vec::with_capacity(path_parts.len());
        for part in path_parts {
            path.push(string_from_bencode(part)?);
        }

        if path.is_empty() {
            return Err(Error::new(ErrorKind::MetainfoInvalidField));
        }

        files.push(FileInfo {
            length: length as u64,
            path,
        });
    }

    Ok(files)
}

/// Parse the optional `announce-list` field (BEP 12).
fn parse_announce_list(val: &Bencode) -> Vec<Vec<String>> {
    let list = match dict_get(val, b"announce-list") {
        Some(Bencode::List(tiers)) => tiers,
        _ => return Vec::new(),
    };

    let mut result = Vec::with_capacity(list.len());
    for tier in list {
        match tier {
            Bencode::List(urls) => {
                let tier_urls: Vec<String> = urls
                    .iter()
                    .filter_map(|u| string_from_bencode(u).ok())
                    .collect();
                if !tier_urls.is_empty() {
                    result.push(tier_urls);
                }
            }
            _ => continue,
        }
    }
    result
}

/// Extract a required string field from a dict.
fn get_required_string(val: &Bencode, key: &[u8]) -> Result<String, Error> {
    let bytes = dict_get_bytes(val, key).ok_or(Error::new(ErrorKind::MetainfoMissingField))?;
    string_from_bencode_bytes(bytes)
}

/// Convert a bencode value to a String, expecting a byte string.
fn string_from_bencode(val: &Bencode) -> Result<String, Error> {
    match val {
        Bencode::Bytes(b) => string_from_bencode_bytes(b),
        _ => Err(Error::new(ErrorKind::MetainfoInvalidField)),
    }
}

/// Convert a byte slice to a String (treats as UTF-8).
fn string_from_bencode_bytes(bytes: &[u8]) -> Result<String, Error> {
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::new(ErrorKind::InvalidInput))
}

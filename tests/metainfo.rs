use bytes::Bytes;
use torrent::bencode::{Bencode, encode};
use torrent::error::ErrorKind;
use torrent::metainfo::{Mode, from_bytes};

fn make_single_file_torrent() -> Vec<u8> {
    let info_dict = Bencode::Dict(vec![
        (Bytes::from("name"), Bencode::Bytes(Bytes::from("test.txt"))),
        (Bytes::from("piece length"), Bencode::Integer(16384)),
        (Bytes::from("length"), Bencode::Integer(1024)),
        (
            Bytes::from("pieces"),
            Bencode::Bytes(Bytes::from(vec![0u8; 20])),
        ),
    ]);
    let root = Bencode::Dict(vec![
        (
            Bytes::from("announce"),
            Bencode::Bytes(Bytes::from("http://tracker.example.com/announce")),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    encode(&root)
}

fn make_multi_file_torrent() -> Vec<u8> {
    let file1 = Bencode::Dict(vec![
        (Bytes::from("length"), Bencode::Integer(512)),
        (
            Bytes::from("path"),
            Bencode::List(vec![
                Bencode::Bytes(Bytes::from("dir1")),
                Bencode::Bytes(Bytes::from("file1.txt")),
            ]),
        ),
    ]);
    let file2 = Bencode::Dict(vec![
        (Bytes::from("length"), Bencode::Integer(512)),
        (
            Bytes::from("path"),
            Bencode::List(vec![
                Bencode::Bytes(Bytes::from("dir2")),
                Bencode::Bytes(Bytes::from("file2.txt")),
            ]),
        ),
    ]);
    let info_dict = Bencode::Dict(vec![
        (
            Bytes::from("name"),
            Bencode::Bytes(Bytes::from("root_folder")),
        ),
        (Bytes::from("piece length"), Bencode::Integer(16384)),
        (
            Bytes::from("pieces"),
            Bencode::Bytes(Bytes::from(vec![0u8; 40])),
        ),
        (
            Bytes::from("files"),
            Bencode::List(vec![file1.into(), file2.into()]),
        ),
    ]);
    let root = Bencode::Dict(vec![
        (
            Bytes::from("announce"),
            Bencode::Bytes(Bytes::from("http://tracker2.example.com/announce")),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    encode(&root)
}

#[test]
fn parse_single_file() {
    let data = make_single_file_torrent();
    let meta = from_bytes(&data).unwrap();
    assert_eq!(meta.announce, "http://tracker.example.com/announce");
    assert_eq!(meta.info.piece_length, 16384);
    assert_eq!(meta.info.pieces.len(), 1);
    assert_eq!(meta.info.total_size(), 1024);
    match &meta.info.mode {
        Mode::Single { name, length } => {
            assert_eq!(name, "test.txt");
            assert_eq!(*length, 1024);
        }
        _ => panic!("expected single file mode"),
    }
}

#[test]
fn parse_multi_file() {
    let data = make_multi_file_torrent();
    let meta = from_bytes(&data).unwrap();
    assert_eq!(meta.announce, "http://tracker2.example.com/announce");
    assert_eq!(meta.info.pieces.len(), 2);
    assert_eq!(meta.info.total_size(), 1024);
    match &meta.info.mode {
        Mode::Multiple { name, files } => {
            assert_eq!(name, "root_folder");
            assert_eq!(files.len(), 2);
            assert_eq!(files[0].path, vec!["dir1", "file1.txt"]);
            assert_eq!(files[1].path, vec!["dir2", "file2.txt"]);
        }
        _ => panic!("expected multi file mode"),
    }
}

#[test]
fn parse_with_optional_fields() {
    let info_dict = Bencode::Dict(vec![
        (Bytes::from("name"), Bencode::Bytes(Bytes::from("test.bin"))),
        (Bytes::from("piece length"), Bencode::Integer(65536)),
        (Bytes::from("length"), Bencode::Integer(999)),
        (
            Bytes::from("pieces"),
            Bencode::Bytes(Bytes::from(vec![0u8; 20])),
        ),
    ]);
    let root = Bencode::Dict(vec![
        (
            Bytes::from("announce"),
            Bencode::Bytes(Bytes::from("http://t.com/ann")),
        ),
        (
            Bytes::from("announce-list"),
            Bencode::List(vec![Bencode::List(vec![
                Bencode::Bytes(Bytes::from("http://t1.com/ann")),
                Bencode::Bytes(Bytes::from("http://t2.com/ann")),
            ])]),
        ),
        (Bytes::from("creation date"), Bencode::Integer(1700000000)),
        (
            Bytes::from("comment"),
            Bencode::Bytes(Bytes::from("test comment")),
        ),
        (
            Bytes::from("created by"),
            Bencode::Bytes(Bytes::from("test-tool-1.0")),
        ),
        (
            Bytes::from("encoding"),
            Bencode::Bytes(Bytes::from("UTF-8")),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    let data = encode(&root);
    let meta = from_bytes(&data).unwrap();

    assert_eq!(meta.announce, "http://t.com/ann");
    assert_eq!(meta.announce_list.len(), 1);
    assert_eq!(meta.announce_list[0].len(), 2);
    assert_eq!(meta.creation_date, Some(1700000000));
    assert_eq!(meta.comment, Some("test comment".to_string()));
    assert_eq!(meta.created_by, Some("test-tool-1.0".to_string()));
    assert_eq!(meta.encoding, Some("UTF-8".to_string()));
}

#[test]
fn compute_info_hash() {
    let data = make_single_file_torrent();
    let meta = from_bytes(&data).unwrap();
    let hash = meta.info_hash();
    assert_eq!(hash.len(), 20);
    // Info hash should be deterministic
    let hash2 = meta.info_hash();
    assert_eq!(hash, hash2);
}

#[test]
fn reject_missing_announce() {
    let info_dict = Bencode::Dict(vec![
        (Bytes::from("name"), Bencode::Bytes(Bytes::from("x"))),
        (Bytes::from("piece length"), Bencode::Integer(16384)),
        (Bytes::from("length"), Bencode::Integer(1)),
        (
            Bytes::from("pieces"),
            Bencode::Bytes(Bytes::from(vec![0u8; 20])),
        ),
    ]);
    let root = Bencode::Dict(vec![(Bytes::from("info"), info_dict)]);
    let data = encode(&root);
    assert!(from_bytes(&data).is_err());
}

#[test]
fn reject_missing_info() {
    let root = Bencode::Dict(vec![(
        Bytes::from("announce"),
        Bencode::Bytes(Bytes::from("http://t.com/a")),
    )]);
    let data = encode(&root);
    assert!(from_bytes(&data).is_err());
}

#[test]
fn reject_invalid_pieces_length() {
    let info_dict = Bencode::Dict(vec![
        (Bytes::from("name"), Bencode::Bytes(Bytes::from("x"))),
        (Bytes::from("piece length"), Bencode::Integer(16384)),
        (Bytes::from("length"), Bencode::Integer(1)),
        (
            Bytes::from("pieces"),
            Bencode::Bytes(Bytes::from(vec![0u8; 15])),
        ),
    ]);
    let root = Bencode::Dict(vec![
        (
            Bytes::from("announce"),
            Bencode::Bytes(Bytes::from("http://t.com/a")),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    let data = encode(&root);
    let result = from_bytes(&data);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::MetainfoInvalidPieces);
}

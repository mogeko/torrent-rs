use torrent_core::bencode::{Bencode, Bytes, encode};
use torrent_core::error::ErrorKind;
use torrent_core::metainfo::{Mode, from_bytes};
use torrent_core::spec::TorrentSpec;

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
        (Bytes::from("files"), Bencode::List(vec![file1, file2])),
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
fn allow_missing_announce_with_announce_list() {
    let info_dict = Bencode::Dict(vec![
        (Bytes::from("name"), Bencode::Bytes(Bytes::from("x"))),
        (Bytes::from("piece length"), Bencode::Integer(16384)),
        (Bytes::from("length"), Bencode::Integer(1)),
        (
            Bytes::from("pieces"),
            Bencode::Bytes(Bytes::from(vec![0u8; 20])),
        ),
    ]);
    let root = Bencode::Dict(vec![
        (
            Bytes::from("announce-list"),
            Bencode::List(vec![Bencode::List(vec![Bencode::Bytes(Bytes::from(
                "http://t1.com/ann",
            ))])]),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    let data = encode(&root);
    let meta = from_bytes(&data).unwrap();
    // announce is empty (key absent) but announce-list is present
    assert_eq!(meta.announce, "");
    assert_eq!(meta.announce_list.len(), 1);
    assert_eq!(meta.announce_list[0][0], "http://t1.com/ann");
}

#[test]
fn allow_missing_announce_standalone() {
    // Torrent with neither announce nor announce-list
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
    // Parses successfully with empty announce (lenient)
    let meta = from_bytes(&data).unwrap();
    assert_eq!(meta.announce, "");
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

// ── Web seed (BEP 19) tests ──

#[test]
fn parse_url_list_single() {
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
            Bencode::Bytes(Bytes::from("http://t.com/ann")),
        ),
        (
            Bytes::from("url-list"),
            Bencode::Bytes(Bytes::from("http://mirror.com/file.iso")),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    let data = encode(&root);
    let meta = from_bytes(&data).unwrap();
    assert_eq!(meta.url_list, vec!["http://mirror.com/file.iso"]);
    assert!(meta.httpseeds.is_empty());
}

#[test]
fn parse_url_list_multiple() {
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
            Bencode::Bytes(Bytes::from("http://t.com/ann")),
        ),
        (
            Bytes::from("url-list"),
            Bencode::List(vec![
                Bencode::Bytes(Bytes::from("http://mirror1.com/file.iso")),
                Bencode::Bytes(Bytes::from("http://mirror2.com/file.iso")),
            ]),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    let data = encode(&root);
    let meta = from_bytes(&data).unwrap();
    assert_eq!(
        meta.url_list,
        vec!["http://mirror1.com/file.iso", "http://mirror2.com/file.iso",]
    );
}

#[test]
fn parse_url_list_trailing_slash() {
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
            Bencode::Bytes(Bytes::from("http://t.com/ann")),
        ),
        (
            Bytes::from("url-list"),
            Bencode::Bytes(Bytes::from("http://mirror.com/pub/")),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    let data = encode(&root);
    let meta = from_bytes(&data).unwrap();
    assert_eq!(meta.url_list, vec!["http://mirror.com/pub/"]);
}

#[test]
fn parse_url_list_missing() {
    let data = make_single_file_torrent();
    let meta = from_bytes(&data).unwrap();
    assert!(meta.url_list.is_empty());
    assert!(meta.httpseeds.is_empty());
}

#[test]
fn parse_httpseeds() {
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
            Bencode::Bytes(Bytes::from("http://t.com/ann")),
        ),
        (
            Bytes::from("httpseeds"),
            Bencode::List(vec![
                Bencode::Bytes(Bytes::from("http://seed1.com/seed.php")),
                Bencode::Bytes(Bytes::from("http://seed2.com/seed.php")),
            ]),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    let data = encode(&root);
    let meta = from_bytes(&data).unwrap();
    assert_eq!(
        meta.httpseeds,
        vec!["http://seed1.com/seed.php", "http://seed2.com/seed.php",]
    );
    assert!(meta.url_list.is_empty());
}

#[test]
fn torrent_spec_web_seeds_from_metainfo() {
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
            Bencode::Bytes(Bytes::from("http://t.com/ann")),
        ),
        (
            Bytes::from("url-list"),
            Bencode::Bytes(Bytes::from("http://mirror.com/file.iso")),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    let data = encode(&root);
    let meta = from_bytes(&data).unwrap();
    let spec = TorrentSpec::from(meta);
    assert_eq!(spec.web_seeds(), vec!["http://mirror.com/file.iso"]);
    assert!(spec.httpseeds().is_empty());
}

// ── FileStatus ──────────────────────────────────────────────

#[test]
fn file_status_single_file_all_complete() {
    let data = make_single_file_torrent();
    let meta = from_bytes(&data).unwrap();
    let bitfield = vec![true]; // 1 piece, all done
    let fs = meta.info.file_status(&bitfield);
    assert_eq!(fs.len(), 1);
    assert_eq!(fs[0].length, 1024);
    assert_eq!(fs[0].downloaded, 1024);
    assert!((fs[0].progress - 1.0).abs() < 0.001);
}

#[test]
fn file_status_single_file_none_complete() {
    let data = make_single_file_torrent();
    let meta = from_bytes(&data).unwrap();
    let bitfield = vec![false];
    let fs = meta.info.file_status(&bitfield);
    assert_eq!(fs.len(), 1);
    assert_eq!(fs[0].downloaded, 0);
    assert!((fs[0].progress - 0.0).abs() < 0.001);
}

#[test]
fn file_status_multi_file() {
    let data = make_multi_file_torrent();
    let meta = from_bytes(&data).unwrap();
    // piece_length=16384, 2 files of 512 each = 1024 total, 2 pieces
    // Only piece 0 covers actual data; piece 1 is past total_size
    let bitfield = vec![true, true];
    let fs = meta.info.file_status(&bitfield);
    assert_eq!(fs.len(), 2);
    assert_eq!(fs[0].length, 512);
    assert_eq!(fs[0].downloaded, 512);
    assert_eq!(fs[1].length, 512);
    assert_eq!(fs[1].downloaded, 512);
}

#[test]
fn file_status_multi_file_partial() {
    let data = make_multi_file_torrent();
    let meta = from_bytes(&data).unwrap();
    let bitfield = vec![false, false]; // 0 pieces done
    let fs = meta.info.file_status(&bitfield);
    assert_eq!(fs.len(), 2);
    assert_eq!(fs[0].downloaded, 0);
    assert_eq!(fs[1].downloaded, 0);
    assert!((fs[0].progress - 0.0).abs() < 0.001);
}

#[test]
fn file_status_multi_file_piece_spans_two_files() {
    // piece_length=200, file1=150 bytes, file2=100 bytes = 250 total, 2 pieces
    // Piece 0: bytes [0, 200) — covers file1 [0, 150) + file2 [0, 50)
    // Piece 1: bytes [200, 250) — covers file2 [50, 100)
    use torrent_core::bencode::Bencode;
    use torrent_core::bencode::{Bytes, encode};

    let file1 = Bencode::Dict(vec![
        (Bytes::from("length"), Bencode::Integer(150)),
        (
            Bytes::from("path"),
            Bencode::List(vec![Bencode::Bytes(Bytes::from("a.txt"))]),
        ),
    ]);
    let file2 = Bencode::Dict(vec![
        (Bytes::from("length"), Bencode::Integer(100)),
        (
            Bytes::from("path"),
            Bencode::List(vec![Bencode::Bytes(Bytes::from("b.txt"))]),
        ),
    ]);
    let info_dict = Bencode::Dict(vec![
        (Bytes::from("name"), Bencode::Bytes(Bytes::from("root"))),
        (Bytes::from("piece length"), Bencode::Integer(200)),
        (Bytes::from("files"), Bencode::List(vec![file1, file2])),
        (
            Bytes::from("pieces"),
            // 2 pieces × 20 bytes each
            Bencode::Bytes(Bytes::from(vec![0u8; 40])),
        ),
    ]);
    let root = Bencode::Dict(vec![
        (
            Bytes::from("announce"),
            Bencode::Bytes(Bytes::from("http://t.com/ann")),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    let data = encode(&root);
    let meta = from_bytes(&data).unwrap();

    // Only piece 0 complete: file1 gets 150, file2 gets first 50
    let bitfield = vec![true, false];
    let fs = meta.info.file_status(&bitfield);
    assert_eq!(fs.len(), 2);
    assert_eq!(fs[0].downloaded, 150);
    assert_eq!(fs[1].downloaded, 50);

    // Both pieces complete
    let bitfield = vec![true, true];
    let fs = meta.info.file_status(&bitfield);
    assert_eq!(fs[0].downloaded, 150);
    assert_eq!(fs[1].downloaded, 100);
}

#[test]
fn file_status_empty_bitfield() {
    let data = make_single_file_torrent();
    let meta = from_bytes(&data).unwrap();
    let bitfield: Vec<bool> = vec![];
    // Should panic — bitfield length must match num_pieces
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        meta.info.file_status(&bitfield)
    }));
    assert!(result.is_err());
}

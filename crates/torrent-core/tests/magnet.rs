use torrent_core::magnet::MagnetUri;

#[test]
fn parse_magnet_hex() {
    let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
    let parsed = uri.parse::<MagnetUri>().unwrap();
    assert_eq!(parsed.info_hashes.len(), 1);
    assert_eq!(
        parsed.info_hashes[0].raw,
        "0123456789abcdef0123456789abcdef01234567"
    );
    let expected: [u8; 20] = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
        0xef, 0x01, 0x23, 0x45, 0x67,
    ];
    assert_eq!(parsed.info_hashes[0].bytes, expected);
}

#[test]
fn parse_magnet_base32() {
    let uri = "magnet:?xt=urn:btih:64wsmv3zsbx5fve2sn5zxdq5w22lfpxy";
    let parsed = uri.parse::<MagnetUri>().unwrap();
    assert_eq!(parsed.info_hashes.len(), 1);
    assert_eq!(
        parsed.info_hashes[0].raw,
        "64wsmv3zsbx5fve2sn5zxdq5w22lfpxy"
    );
    assert_eq!(parsed.info_hashes[0].bytes.len(), 20);
}

#[test]
fn parse_magnet_with_dn() {
    let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=Ubuntu+24.04";
    let parsed = uri.parse::<MagnetUri>().unwrap();
    assert_eq!(parsed.display_name, Some("Ubuntu+24.04".to_string()));
}

#[test]
fn parse_magnet_with_trackers() {
    let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
        &tr=http://tracker1.com:80/announce\
        &tr=http://tracker2.com:80/announce";
    let parsed = uri.parse::<MagnetUri>().unwrap();
    assert_eq!(parsed.trackers.len(), 2);
    assert_eq!(parsed.trackers[0], "http://tracker1.com:80/announce");
    assert_eq!(parsed.trackers[1], "http://tracker2.com:80/announce");
}

#[test]
fn parse_magnet_multiple_xt() {
    let uri = "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
        &xt=urn:btih:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let parsed = uri.parse::<MagnetUri>().unwrap();
    assert_eq!(parsed.info_hashes.len(), 2);
}

#[test]
fn reject_magnet_without_xt() {
    let uri = "magnet:?dn=test";
    assert!(uri.parse::<MagnetUri>().is_err());
}

#[test]
fn reject_invalid_prefix() {
    assert!("http://example.com".parse::<MagnetUri>().is_err());
}

#[test]
fn roundtrip_display() {
    let uri_str = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
        &dn=test+file&tr=http://tracker.com/announce";
    let parsed = uri_str.parse::<MagnetUri>().unwrap();
    let displayed = parsed.to_string();
    // xt hash (hex chars are unreserved — no encoding)
    assert!(displayed.contains("xt=urn:btih:0123456789abcdef0123456789abcdef01234567"));
    // + and :/// are percent-encoded per RFC 3986
    assert!(displayed.contains("dn=test%2Bfile"));
    assert!(displayed.contains("tr=http%3A%2F%2Ftracker.com%2Fannounce"));
    // Values survive round-trip
    let parsed2 = displayed.parse::<MagnetUri>().unwrap();
    assert_eq!(parsed, parsed2);
}

#[test]
fn parse_magnet_case_insensitive_prefix() {
    let uri = "MAGNET:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    assert!(uri.parse::<MagnetUri>().is_ok());
}

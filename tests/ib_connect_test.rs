use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Encode u64 as protobuf varint
fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
    buf
}

/// Build StartApiRequest protobuf payload: field 1 (client_id) as varint
fn start_api_payload(client_id: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    // tag for field 1, wire type 0: (1 << 3) | 0 = 8
    payload.push(0x08);
    payload.extend_from_slice(&encode_varint(client_id as u64));
    payload
}

#[tokio::test]
#[ignore = "Debug IB connect"]
async fn test_post_handshake_messages_correct() {
    let addr = "127.0.0.1:4003";
    let client_id = 999;

    println!("Connecting to {}", addr);
    let mut stream = TcpStream::connect(addr).await.unwrap();
    println!("TCP connected");

    // Send handshake
    let version = b"v100..225";
    let mut handshake = Vec::from(b"API\0");
    handshake.extend_from_slice(&(version.len() as u32).to_be_bytes());
    handshake.extend_from_slice(version);
    stream.write_all(&handshake).await.unwrap();
    stream.flush().await.unwrap();
    println!("Handshake sent");

    // Read handshake response
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut resp = vec![0u8; len];
    stream.read_exact(&mut resp).await.unwrap();
    println!("Handshake response: {:?}", String::from_utf8_lossy(&resp));

    // Send start_api
    let proto_payload = start_api_payload(client_id);
    let msg_id = (ibapi::messages::OutgoingMessages::StartApi as i32) + ibapi::messages::PROTOBUF_MSG_ID;
    let payload_len = 4 + proto_payload.len();
    let mut packet = Vec::new();
    packet.extend_from_slice(&(payload_len as u32).to_be_bytes());
    packet.extend_from_slice(&msg_id.to_be_bytes());
    packet.extend_from_slice(&proto_payload);

    println!("Sending start_api: msg_id={}, payload={:?}", msg_id, proto_payload);
    stream.write_all(&packet).await.unwrap();
    stream.flush().await.unwrap();
    println!("start_api sent, waiting for responses...");

    // Read for 15 seconds
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(15) {
        let mut buf = [0u8; 4096];
        match tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await {
            Ok(Ok(0)) => { println!("Connection closed"); break; }
            Ok(Ok(n)) => {
                if n >= 4 {
                    let msg_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                    if n >= 4 + msg_len {
                        let body = &buf[4..4+msg_len];
                        if body.len() >= 4 {
                            let id = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                            println!("Response msg_id={}, len={}, data={:?}", id, msg_len, &body[4..body.len().min(32)]);
                        } else {
                            println!("Response text-like: {:?}", String::from_utf8_lossy(body));
                        }
                    }
                }
            }
            Ok(Err(e)) => { println!("Read error: {}", e); break; }
            Err(_) => {}
        }
    }
    println!("Done reading");
}

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let listener = TcpListener::bind("localhost:8080").await.unwrap();
    let (mut socket, _addr) = listener.accept().await.unwrap();
    let (read, mut write) = socket.split();
    let mut reader = BufReader::new(read);
    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line).await.unwrap();
        if bytes_read == 0 {
            break;
        }
        write.write_all(&line.as_bytes()).await.unwrap();
    }
}

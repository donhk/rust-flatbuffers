use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let listener = TcpListener::bind("localhost:8080").await.unwrap();
    let (tx, _rx) = broadcast::channel(10);
    loop {
        let (mut socket, addr) = listener.accept().await.unwrap();
        let tx = tx.clone();
        let mut rx = tx.subscribe();
        tokio::spawn(async move {
            let (read, mut write) = socket.split();
            let mut reader = BufReader::new(read);
            let mut line = String::new();
            loop {
                tokio::select! {
                    result = reader.read_line(&mut line) => {
                        if result.unwrap() == 0{
                            break;
                        }
                        tx.send((line.clone(), addr)).unwrap();
                        line.clear();
                    },
                    result = rx.recv() => {
                        let (result, other_addr) = result.unwrap();
                        if other_addr != addr {
                            write.write_all(result.as_bytes()).await.unwrap();
                        }
                    }
                }
            }
        });
    }
}

use multiprocessing::tokio::{channel, duplex, Duplex, Receiver, Sender};
use multiprocessing::SerializeSafe;

#[derive(Debug, PartialEq, SerializeSafe)]
struct SimplePair {
    x: i32,
    y: i32,
}

#[multiprocessing::entrypoint]
#[tokio::main]
async fn simple() -> i64 {
    0x123456789abcdef
}

#[multiprocessing::entrypoint]
#[tokio::main]
async fn add_with_arguments(x: i32, y: i32) -> i32 {
    x + y
}

#[multiprocessing::entrypoint]
#[tokio::main]
async fn swap_complex_argument(pair: SimplePair) -> SimplePair {
    SimplePair {
        x: pair.y,
        y: pair.x,
    }
}

#[multiprocessing::entrypoint]
#[tokio::main]
async fn with_passed_rx(mut rx: Receiver<i32>) -> i32 {
    let a = rx.recv().await.unwrap().unwrap();
    let b = rx.recv().await.unwrap().unwrap();
    a - b
}

#[multiprocessing::entrypoint]
#[tokio::main]
async fn with_passed_tx(mut tx: Sender<i32>) -> () {
    tx.send(&5).await.unwrap();
    tx.send(&7).await.unwrap();
}

#[multiprocessing::entrypoint]
#[tokio::main]
async fn with_passed_duplex(mut chan: Duplex<i32, (i32, i32)>) -> () {
    while let Some((x, y)) = chan.recv().await.unwrap() {
        chan.send(&(x - y)).await.unwrap();
    }
}

#[multiprocessing::main]
#[tokio::main]
async fn main() {
    assert_eq!(
        simple::spawn().unwrap().join().expect("simple failed"),
        0x123456789abcdef
    );
    println!("simple OK");

    assert_eq!(
        add_with_arguments::spawn(5, 7)
            .unwrap()
            .join()
            .expect("add_with_arguments failed"),
        12
    );
    println!("add_with_arguments OK");

    assert_eq!(
        swap_complex_argument::spawn(SimplePair { x: 5, y: 7 })
            .unwrap()
            .join()
            .expect("swap_complex_argument failed"),
        SimplePair { x: 7, y: 5 }
    );
    println!("swap_complex_argument OK");

    {
        let (mut tx, rx) = channel::<i32>().unwrap();
        let mut child = with_passed_rx::spawn(rx).unwrap();
        tx.send(&5).await.unwrap();
        tx.send(&7).await.unwrap();
        assert_eq!(child.join().expect("with_passed_rx failed"), -2);
        println!("with_passed_rx OK");
    }

    {
        let (tx, mut rx) = channel::<i32>().unwrap();
        let mut child = with_passed_tx::spawn(tx).unwrap();
        assert_eq!(
            rx.recv().await.unwrap().unwrap() - rx.recv().await.unwrap().unwrap(),
            -2
        );
        child.join().unwrap();
        println!("with_passed_tx OK");
    }

    {
        let (mut local, downstream) = duplex::<(i32, i32), i32>().unwrap();
        let mut child = with_passed_duplex::spawn(downstream).unwrap();
        for (x, y) in [(5, 7), (100, -1), (53, 2354)] {
            local.send(&(x, y)).await.unwrap();
            assert_eq!(local.recv().await.unwrap().unwrap(), x - y);
        }
        drop(local);
        child.join().unwrap();
        println!("with_passed_duplex OK");
    }
}

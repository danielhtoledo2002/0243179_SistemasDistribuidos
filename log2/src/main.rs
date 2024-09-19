mod comp {
    pub mod config;
    pub mod index;
    pub mod log;
    pub mod record;
    pub mod segments;
    pub mod store;
}
use comp::config::Config;
use comp::index::Index;
// use comp::log::Log;
use comp::segments::Segment;
use comp::store::Store;
use std::error::Error;
use tokio;
use tokio::fs::OpenOptions;
use tokio::net::TcpListener;
use tokio::task::spawn_blocking;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let file_path = "temp_file.bin";
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)
        .await?;

    let mut store = Store::new(file, file_path.to_string()).await?;

    let data = b"hello  hasldkfhjalsdk  ";

    let (n, pos) = store.append(data).await?;
    println!("Data appended at position: {}, bytes written: {}", pos, n);

    let read_data = store.read(pos).await?;
    println!("Data read: {:?}", String::from_utf8(read_data)?);

    let data = b"hello";

    let (n, pos) = store.append(data).await?;
    println!("Data appended at position: {}, bytes written: {}", pos, n);

    let mut read_data = store.read(pos).await?;
    println!("Data read: {:?}", String::from_utf8(read_data.clone())?);
    
    
    println!("EL tamaño es de {}", store.reat_at(&mut read_data[..], pos).await.unwrap() as u64);

    store.close().await?;
    println!("Store closed successfully.");

    Ok(())
}

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use crate::comp::config::SegmentConfig;
use crate::comp::record::Record;

// #[derive(Clone)]
// struct Log {
//     segments: Vec<RwLock<Segment>>,
// }

// struct Segment {
//     store: Box<Store>,
//     index: Box<Index>,
// }

// #[derive(Clone)]
// struct AppState {
//     logger: RwLock<Log>,
// }

// #[tokio::main]
// async fn main() {
//     let state = Arc::new(AppState { logger: todo!() });

//     let router = Router::new()
//         .route("/get/:id", get(get_log))
//         .with_state(state);

//     let tcp = TcpListener::bind("[::]:8080").await.unwrap();

//     axum::serve(tcp, router).await.unwrap()
// }

// async fn get_log(State(a): State<AppState>) {
//     let a = a.logger.read().unwrap();
// }

// #[tokio::main]
// async fn main() -> io::Result<()> {
//     let _ = spawn_blocking(|| -> io::Result<Index> {
//         let path = Path::new("index_file.bin");

//         let name = "index_file.bin".to_string();
//         let config = Config {
//             segment: SegmentConfig {
//                 max_store_bytes: 12,
//                 max_index_bytes: 1024,
//                 initial_offset: 4,
//             },
//         };

//         let mut file = File::create_new(path)?;

//         let mut index = Index::new(&file, &config, name)?;

//         index.write(1, 100)?;
//         index.write(2, 4)?;

//         match index.read(-1) {
//             Ok((offset, position)) => {
//                 println!("Offset: {}, Position: {}", offset, position);
//             }
//             Err(e) => {
//                 println!("Error reading index: {}", e);
//             }
//         }

//         index.close(&mut file)?;
//         Ok(index)
//     })
//     .await
//     .unwrap();

//     Ok(())
// }

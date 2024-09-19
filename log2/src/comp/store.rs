use std::io::{self, Error};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom};

use byteorder::{BigEndian, WriteBytesExt};

// voy a manejar el mutex Mutex::new(Store) ya que asi me evito pedos.

pub const LEN_WIDTH: usize = 8;

#[derive(Debug)]
pub struct Store {
    pub reader: BufReader<File>,
    pub writer: BufWriter<File>,
    pub size: u64,
    pub path: String,
}

impl Store {
    pub async fn new(file: File, path: String) -> io::Result<Store> {
        let metadata = file.metadata().await?;
        let size = metadata.len();
        let reader = BufReader::new(file.try_clone().await?);
        let writer = BufWriter::new(file);

        Ok(Store {
            reader,
            writer,
            size,
            path,
        })
    }

    pub async fn append(&mut self, p: &[u8]) -> io::Result<(u64, u64)> {
        // Hacemos que apunte a la dirección donde quiere escribir el archivo en este caso
        // La ultima posición de el archivo

        match self.writer.seek(SeekFrom::Start(self.size)).await {
            Ok(_) => {}
            Err(e) => {
                println!(
                    "Error no existe la dirección donde se quiere escribir {}",
                    e
                );
                return Err(e);
            }
        };

        let size = (p.len() as u64).to_be_bytes();

        self.writer.write_all(&size).await?;
        let pos = self.size;

        // Actualizamos el tamaño

        let mut bytes_written = self.writer.write(p).await? as u64;
        bytes_written += LEN_WIDTH as u64;
        self.writer.flush().await?;
        self.size += bytes_written as u64;

        Ok((bytes_written, pos))
    }

    pub async fn read(&mut self, pos: u64) -> io::Result<Vec<u8>> {
        // el flush para saber que ya acabo de escribir
        match self.writer.flush().await {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        };
        match self.reader.seek(SeekFrom::Start(pos)).await {
            Ok(_) => {}
            Err(e) => {
                println!("Error no existe la dirección donde se quiere leer {}", e);
                return Err(e);
            }
        };

        let mut buf = [0u8; LEN_WIDTH];

        // print preventivo para ver que chingados estaba leyendo
        // println!("{:?}", self.reader);

        match self.reader.read_exact(&mut buf).await {
            Ok(_) => {}
            Err(e) => {
                println!("Error al leer los datos {}", e);
            }
        };

        println!("El bufer normal es {:?}", buf);

        let size = u64::from_be_bytes(buf);

        let mut data_buf = vec![0u8; size as usize];

        match self.reader.read_exact(&mut data_buf).await {
            Ok(_) => Ok(data_buf),
            Err(e) => {
                println!("Error al leer los datos {}", e);
                Err(e)
            }
        }
    }

    pub async fn name(self) -> String {
        self.path
    }

    pub async fn close(&mut self) -> io::Result<()> {
        self.path = "".to_string();
        match self.writer.flush().await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        };
        match self.reader.flush().await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

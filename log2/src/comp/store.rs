use std::io;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom};

// voy a manejar el mutex Mutex::new(Store) ya que asi me evito pedos.

pub const LEN_WIDTH: usize = 8;

#[derive(Debug)]
pub struct Store {
    reader: BufReader<File>,
    writer: BufWriter<File>,
    size: u64,
}

impl Store {
    pub async fn new(file: File) -> io::Result<Store> {
        let metadata = file.metadata().await?;
        let size = metadata.len();
        let reader = BufReader::new(file.try_clone().await?);
        let writer = BufWriter::new(file);

        Ok(Store {
            reader,
            writer,
            size,
        })
    }

    pub async fn append(&mut self, p: &[u8]) -> io::Result<(u64, u64)> {
        // Hacemos que apunte a la dirección donde quiere escribir el archivo en este caso
        // La ultima posición de el archivo
        match self.writer.seek(SeekFrom::Start(self.size)).await {
            Ok(_) => {}
            Err(e) => {
                println!("Error no existe la dirección donde se quiere leer {}", e);
                return Err(e);
            }
        };

        match self.writer.write_all(p).await {
            Ok(_) => {}

            Err(e) => {
                println!("Fallo al intentar escribribir {e}");
                return Err(e);
            }
        };

        // Ya no es necesario el flush ya que el metodo write_all se va a encargar de escribir todo
        /*
        match self.writer.flush().await {
            Ok(_) => {}

            Err(e) => {
                println!("Fallo al intentar escribir {}", e);
                return Err(e);
            }
        };
        */

        // leemos el tamaño de lo que escribimos
        let w = p.len() as u64;
        // Actualizamos el tamaño
        self.size += w;

        Ok((self.size, w))
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

        let mut buf = vec![0u8; LEN_WIDTH];

        // print preventivo para ver que chingados estaba leyendo
        // println!("{:?}", self.reader);

        match self.reader.read_exact(&mut buf).await {
            Ok(_) => Ok(buf),
            Err(e) => {
                println!("Error al leer los datos {}", e);
                Err(e)
            }
        }
    }

    pub async fn close(mut self) -> io::Result<()> {
        match self.writer.flush().await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::Deref;
use std::path::Path;
use std::thread;

use crossbeam_channel::{bounded, Sender};
use crossbeam_utils::sync::Parker;

pub struct FileCollector<T: Copy + Send> {
    file: File,
    tx: Sender<T>,
    lock: Parker,
}

impl<T: 'static + Copy + Send + Deref<Target = [u8]>> FileCollector<T> {
    pub fn new<P: AsRef<Path>>(
        filename: P,
        buffer: usize,
    ) -> Result<FileCollector<T>, std::io::Error> {
        let file = File::create(filename)?;
        let (tx, rx) = bounded::<T>(buffer);
        let lock = Parker::new();
        let u = lock.unparker().clone();

        let mut bufwrite = BufWriter::new(file.try_clone()?);
        thread::spawn(move || {
            for x in rx.iter() {
                println!("got msg");
                bufwrite.write_all(&*x).unwrap()
            }
            println!("done");
            bufwrite.flush().unwrap();
            drop(rx);
            drop(bufwrite);
            u.unpark();
        });

        Ok(FileCollector { file, tx, lock })
    }

    pub fn sink(&self) -> Sender<T> {
        self.tx.clone()
    }

    pub fn finish(self) {
        drop(self.tx);
        self.lock.park();
        drop(self.file);
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use temp_testdir::TempDir;

    use crate::FileCollector;

    #[test]
    fn basic() {
        let temp = TempDir::default();
        let mut file_path = PathBuf::from(temp.as_ref());
        file_path.push("test_basic");

        let collector: FileCollector<&[u8]> = FileCollector::new(&file_path, 1024).unwrap();
        let sink = collector.sink();
        sink.send(b"foobar").unwrap();
        drop(sink);
        collector.finish();

        assert_eq!(b"foobar", &std::fs::read(&file_path).unwrap()[..]);
    }
}

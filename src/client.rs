use api::shm_client::ShmClient;
use arrow_array::{
    ArrayRef, BooleanArray, Int32Array, RecordBatch, RecordBatchWriter, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use memmap2::MmapMut;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use tokio::net::UnixStream;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;

#[tokio::main]
async fn main() {
    let sock_file = "/tmp/greptimedb.sock";

    // We will ignore this uri because uds do not use it if your connector does use the uri it will
    // be provided as the request to the `MakeConnection`.
    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| {
            // Connect to a Uds socket
            UnixStream::connect(sock_file)
        }))
        .await
        .unwrap();

    let mut client = ShmClient::new(channel);

    // write batches to local file.
    let (data_file, start, end) = write_batch();
    // notify server to ingest that file
    let resp = client
        .notify(api::Notification {
            file_name: data_file.clone(),
            start: i32::try_from(start).unwrap(),
            end: i32::try_from(end).unwrap(),
        })
        .await
        .unwrap();

    // if server has successfully ingested that file, we can safely delete it.
    if resp.into_inner().success {
        println!("Server has ingested file: {}", data_file);
        std::fs::remove_file(data_file).unwrap();
    }
}

fn write_batch() -> (String, usize, usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name".to_string(), DataType::Utf8, false),
        Field::new("age".to_string(), DataType::Int32, false),
        Field::new("adult".to_string(), DataType::Boolean, false),
    ]));

    let batch_to_write = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![16, 17, 18])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false, false, true])) as ArrayRef,
        ],
    )
    .unwrap();

    let file_prefix = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_file = format!("/tmp/{}", file_prefix);

    // todo: maybe we can reuse this file as a ring buffer.
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(data_file.clone())
        .unwrap();

    file.set_len(1024 * 1024 * 4).unwrap();

    let mut mapped = unsafe { MmapMut::map_mut(&file).unwrap() };

    let (writer, size) = SizedWriterBuffer::new(mapped.deref_mut());
    // write record batch to data file.
    let mut writer = arrow_ipc::writer::FileWriter::try_new(writer, &*schema).unwrap();
    writer.write(&batch_to_write).unwrap();
    writer.close().unwrap();

    (data_file, 0, size.load(Ordering::Relaxed))
}

/// Simple wrapper of `Write` so that we can get how many bytes are written.
struct SizedWriterBuffer<T> {
    inner: T,
    size: Arc<AtomicUsize>,
}

impl<T> std::io::Write for SizedWriterBuffer<T>
where
    T: std::io::Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf).map(|size| {
            self.size.fetch_add(size, Ordering::Relaxed);
            size
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<T> SizedWriterBuffer<T> {
    fn new(inner: T) -> (Self, Arc<AtomicUsize>) {
        let counter: Arc<AtomicUsize> = Arc::new(Default::default());
        (
            Self {
                inner,
                size: counter.clone(),
            },
            counter,
        )
    }
}

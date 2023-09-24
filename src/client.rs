use api::shm_client::ShmClient;
use arrow_array::{
    ArrayRef, BooleanArray, Int32Array, RecordBatch, RecordBatchWriter, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

#[tokio::main]
async fn main() {
    let addr = "http://127.0.0.1:4013".to_string();
    let mut client: ShmClient<tonic::transport::Channel> = ShmClient::connect(addr).await.unwrap();

    // write batches to local file.
    let data_file = write_batch();
    // notify server to ingest that file
    let resp = client
        .notify(api::Notification {
            file_name: data_file.clone(),
        })
        .await
        .unwrap();

    // if server has successfully ingested that file, we can safely delete it.
    if resp.into_inner().success {
        println!("Server has ingested file: {}", data_file);
        std::fs::remove_file(data_file).unwrap();
    }
}

fn write_batch() -> String {
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
    // write record batch to data file.
    let mut writer = arrow_ipc::writer::FileWriter::try_new(
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(data_file.clone())
            .unwrap(),
        &*schema,
    )
    .unwrap();
    writer.write(&batch_to_write).unwrap();
    writer.close().unwrap();
    data_file
}

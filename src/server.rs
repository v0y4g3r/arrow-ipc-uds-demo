use api::shm_server::{Shm, ShmServer};
use api::{IngestResponse, Notification};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

struct ShmServerImpl {}

impl ShmServerImpl {
    pub async fn start() {
        let path = "/tmp/greptimedb.sock";
        println!("Starting client at {}", path);
        let uds_stream = UnixListenerStream::new(UnixListener::bind(path).unwrap());

        tonic::transport::Server::builder()
            .add_service(ShmServer::new(ShmServerImpl {}))
            .serve_with_incoming(uds_stream)
            .await
            .unwrap();
    }
}

#[tonic::async_trait]
impl Shm for ShmServerImpl {
    async fn notify(
        &self,
        request: Request<Notification>,
    ) -> Result<Response<IngestResponse>, Status> {
        let notification = request.into_inner();
        println!("{}", notification.file_name);

        let mut reader = arrow_ipc::reader::FileReader::try_new(
            std::fs::OpenOptions::new()
                .read(true)
                .open(notification.file_name)
                .unwrap(),
            None,
        )
        .unwrap();

        // read file
        while let Some(batch) = reader.next() {
            println!("{:?}", batch.unwrap());
        }

        Ok(Response::new(IngestResponse { success: true }))
    }
}

#[tokio::main]
async fn main() {
    ShmServerImpl::start().await;
}

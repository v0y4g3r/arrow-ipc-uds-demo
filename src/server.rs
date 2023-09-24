use api::shm_server::{Shm, ShmServer};
use api::{IngestResponse, Notification};
use tonic::{Request, Response, Status};

struct ShmServerImpl {}

impl ShmServerImpl {
    pub async fn start(addr: String) {
        let addr = addr.parse().unwrap();
        println!("Starting client at {}", addr);
        tonic::transport::Server::builder()
            .add_service(ShmServer::new(ShmServerImpl {}))
            .serve(addr)
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
    ShmServerImpl::start("127.0.0.1:4013".to_string()).await;
}

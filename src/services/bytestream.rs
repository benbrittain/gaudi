use crate::{api, content_storage::ContentStorage};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

#[derive(Debug)]
pub struct BytestreamService {
    content_store: ContentStorage,
}

impl BytestreamService {
    pub fn new(content_store: ContentStorage) -> Self {
        BytestreamService { content_store }
    }
}

#[tonic::async_trait]
impl api::ByteStream for BytestreamService {
    type ReadStream = ReceiverStream<Result<api::ReadResponse, Status>>;

    #[instrument(skip(self))]
    async fn read(
        &self,
        request: Request<api::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        // TODO variable read offsets
        assert_eq!(0, request.get_ref().read_offset);
        assert_eq!(0, request.get_ref().read_limit);
        let resource_name = &request.get_ref().resource_name;

        let segments: Vec<&str> = resource_name.split("/").collect();
        let instance = segments[0];
        assert_eq!("blobs", segments[1]);
        let hash = segments[2];
        let len = segments[3];
        info!("Reading");

        let (tx, rx) = mpsc::channel(128);
        let content_store = self.content_store.clone();
        let instance = instance.clone().to_owned();
        let hash = hash.clone().to_owned();
        tokio::spawn(async move {
            if let Ok(buf) = content_store.read_to_end(&instance, &hash).await {
                info!("blob size {:?}", buf.len());
                let op = api::ReadResponse { data: buf };
                tx.send(Result::<_, Status>::Ok(op)).await.unwrap();
                info!("Read.");
            } else {
                info!("Did not Read!");
                tx.send(Result::<_, Status>::Err(Status::new(
                    tonic::Code::Unknown,
                    "oh no",
                )))
                .await
                .unwrap();
            }
        });
        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(output_stream as Self::ReadStream))
    }

    #[instrument(skip_all)]
    async fn write(
        &self,
        request: Request<tonic::Streaming<api::WriteRequest>>,
    ) -> Result<Response<api::WriteResponse>, Status> {
        let mut stream = request.into_inner();
        let mut size: usize = 0;
        if let Some(write_req) = stream.message().await? {
            info!("Name: {:?}", &write_req.resource_name);
            let segments: Vec<&str> = write_req.resource_name.split("/").collect();
            let instance = segments[0];
            assert_eq!("uploads", segments[1]);
            let uuid = uuid::Uuid::parse_str(segments[2])
                .map_err(|_| Status::invalid_argument("not a valid uuid"))?;
            assert_eq!("blobs", segments[3]);
            let hash = segments[4];
            let data_size: usize = segments[5]
                .parse()
                .map_err(|_| Status::invalid_argument("bad size value"))?;

            info!("Writing to blob");
            let bytes_written = self
                .content_store
                .write_data(
                    instance,
                    uuid,
                    hash,
                    data_size,
                    write_req.write_offset,
                    write_req.finish_write,
                    &write_req.data,
                )
                .await
                .map_err(|_| Status::internal("content store could not write data"))?;
            info!("Bytes written: {}", bytes_written);
            size += bytes_written;
        }
        Ok(Response::new(api::WriteResponse {
            committed_size: size as i64,
        }))
    }

    #[instrument(skip_all, fields(resource = _request.get_ref().resource_name))]
    async fn query_write_status(
        &self,
        _request: Request<api::QueryWriteStatusRequest>,
    ) -> Result<Response<api::QueryWriteStatusResponse>, Status> {
        info!("Checking...");
        Err(Status::not_found("BWB TODO"))
    }
}

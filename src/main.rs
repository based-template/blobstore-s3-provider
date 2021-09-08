//! blobstore-s3-provider capability provider
//!
//!
use blobstore_interface::{
    BlobList, Blobstore, BlobstoreReceiver, BlobstoreResult, Container, FileBlob, FileChunk,
    GetObjectInfoRequest, RemoveObjectRequest, StartDownloadRequest,
};
use hyper::{Client, Uri};
use hyper_proxy::{Intercept, Proxy, ProxyConnector};
use hyper_tls::HttpsConnector;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use rusoto_core::credential::{DefaultCredentialsProvider, StaticProvider};
use rusoto_core::Region;
use rusoto_s3::{
    CreateBucketRequest, DeleteBucketRequest, ListObjectsV2Output, ListObjectsV2Request, Object,
    S3Client, S3,
};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, RwLock},
};
use wasmbus_rpc::provider::prelude::*;

type HttpConnector =
    hyper_proxy::ProxyConnector<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>;
//use wasmcloud_interface_factorial::{Factorial, FactorialReceiver};

// main (via provider_main) initializes the threaded tokio executor,
// listens to lattice rpcs, handles actor links,
// and returns only when it receives a shutdown message
//
fn main() -> Result<(), Box<dyn std::error::Error>> {
    provider_main(BlobstoreS3ProviderProvider::default())?;

    eprintln!("blobstore-s3-provider provider exiting");
    Ok(())
}

/// blobstore-s3-provider capability provider implementation
#[derive(Default, Clone)]
struct BlobstoreS3ProviderProvider {
    clients: Arc<RwLock<HashMap<String, S3Client>>>,
}

fn client_for_config(config_map: &HashMap<String, String>) -> RpcResult<S3Client> {
    let region = if config_map.contains_key("REGION") {
        Region::Custom {
            name: config_map["REGION"].clone(),
            endpoint: if config_map.contains_key("ENDPOINT") {
                config_map["ENDPOINT"].clone()
            } else {
                "s3.us-east-1.amazonaws.com".to_string()
            },
        }
    } else {
        Region::UsEast1
    };

    let client = if config_map.contains_key("AWS_ACCESS_KEY") {
        let provider = StaticProvider::new(
            config_map["AWS_ACCESS_KEY"].to_string(),
            config_map["AWS_SECRET_ACCESS_KEY"].to_string(),
            config_map.get("AWS_TOKEN").cloned(),
            config_map
                .get("TOKEN_VALID_FOR")
                .map(|t| t.parse::<i64>().unwrap()),
        );
        let connector: HttpConnector = if let Some(proxy) = config_map.get("HTTP_PROXY") {
            let proxy = Proxy::new(Intercept::All, proxy.parse::<Uri>().unwrap());
            ProxyConnector::from_proxy(hyper_tls::HttpsConnector::new(), proxy).unwrap()
        } else {
            ProxyConnector::new(HttpsConnector::new()).unwrap()
        };
        let mut hyper_builder: hyper::client::Builder = Client::builder();
        hyper_builder.pool_max_idle_per_host(0);
        let client = rusoto_core::HttpClient::from_builder(hyper_builder, connector);
        S3Client::new_with(client, provider, region)
    } else {
        let provider = DefaultCredentialsProvider::new().unwrap();
        S3Client::new_with(
            rusoto_core::request::HttpClient::new()
                .expect("Failed to create HTTP client for S3 provider"),
            provider,
            region,
        )
    };

    Ok(client)
}

/// use default implementations of provider message handlers
impl ProviderDispatch for BlobstoreS3ProviderProvider {}
impl BlobstoreReceiver for BlobstoreS3ProviderProvider {}
#[async_trait]
impl ProviderHandler for BlobstoreS3ProviderProvider {
    async fn put_link(&self, ld: &LinkDefinition) -> RpcResult<bool> {
        // self.clients.write().unwrap().insert(config.module.clone(), Arc::new(s3::client_for_config(&config)?),);
        // TODO: write client_for_config!!!
        self.clients
            .write()
            .unwrap()
            .insert(ld.actor_id.to_string(), client_for_config(&ld.values)?);
        Ok(true)
    }

    async fn delete_link(&self, actor_id: &str) {
        // self.clients.write().unwrap().remove(_actor_id)
        self.clients.write().unwrap().remove(actor_id);
    }

    async fn shutdown(&self) -> Result<(), Infallible> {
        Ok(())
    }
}

/// Handle Blobstore methods
#[async_trait]
impl Blobstore for BlobstoreS3ProviderProvider {
    /// creates a new container
    async fn create_container<TS: ToString + ?Sized + std::marker::Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<Container> {
        let container_id = arg.to_string();
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?;
        //let s3_client = &self.clients.read().unwrap()[actor_id];
        let rd = self.clients.read().unwrap();
        //.ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?;
        let s3_client = rd
            .get(actor_id)
            .ok_or_else(|| RpcError::InvalidParameter(format!("actor not linked:{}", actor_id)))?;
        /*
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(s3_client.create_bucket(create_bucket_req))
            .map_err(|_e| RpcError::Other("create_bucket() failed".to_string()))?;
         */
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(create_bucket(container_id.clone(), s3_client))?;
        Ok(Container {
            id: container_id.clone(),
        })
        //let rt = tokio::runtime::Runtime::new().unwrap();
        //rt.block_on()
        //Ok(build_empty_container())
    }

    /// RemoveContainer(id: string): BlobstoreResult
    async fn remove_container<TS: ToString + ?Sized + std::marker::Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<BlobstoreResult> {
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?;
        let bucket_id = arg.to_string();
        let rd = self.clients.read().unwrap();
        let s3_client = rd
            .get(actor_id)
            .ok_or_else(|| RpcError::InvalidParameter(format!("actor not linked:{}", actor_id)))?;
        let rt = tokio::runtime::Runtime::new().unwrap();
        Ok(rt
            .block_on(remove_bucket(bucket_id, s3_client))
            .map_or_else(
                |e| BlobstoreResult {
                    success: false,
                    error: Some(e.to_string()),
                },
                |_| BlobstoreResult {
                    success: true,
                    error: None,
                },
            ))
    }

    /// remove_object()
    async fn remove_object(
        &self,
        _ctx: &Context,
        _arg: &RemoveObjectRequest,
    ) -> RpcResult<BlobstoreResult> {
        Ok(build_empty_blobstore_result())
    }

    /// list_objects(container_id: string): BlobList
    async fn list_objects<TS: ToString + ?Sized + std::marker::Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<BlobList> {
        let container_id = arg.to_string();
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let objects = rt
            .block_on(list_objects(
                container_id,
                &self.clients.read().unwrap()[actor_id],
            ))
            .unwrap();
        let blobs = if let Some(v) = objects {
            v.iter()
                .map(|ob| FileBlob {
                    id: ob.key.clone().unwrap(),
                    container: Container {
                        id: arg.to_string().clone(),
                    },
                    byte_size: ob.size.unwrap() as u64,
                })
                .collect()
        } else {
            BlobList::new()
        };
        Ok(blobs)
    }

    /// upload_chunk(chunk: FileChunk): BlobstoreResult
    async fn upload_chunk(&self, _ctx: &Context, _arg: &FileChunk) -> RpcResult<BlobstoreResult> {
        Ok(build_empty_blobstore_result())
    }

    /// start_download(blob_id: string, container_id: string, chunk_size: u64, context: string?): BlobstoreResult
    async fn start_download(
        &self,
        _ctx: &Context,
        _arg: &StartDownloadRequest,
    ) -> RpcResult<BlobstoreResult> {
        Ok(build_empty_blobstore_result())
    }

    /// start_upload(self, ctx, arg: &FileChunk) -> RpcResult<BlobstoreResult>
    async fn start_upload(&self, _ctx: &Context, _arg: &FileChunk) -> RpcResult<BlobstoreResult> {
        Ok(build_empty_blobstore_result())
    }

    /// get_object_info(self, ctx, GetObjectInfoRequest) -> RpcResult<FileBlob>
    async fn get_object_info(
        &self,
        _ctx: &Context,
        _arg: &GetObjectInfoRequest,
    ) -> RpcResult<FileBlob> {
        Ok(build_empty_fileblob())
    }
}

async fn create_bucket(container_id: String, client: &S3Client) -> RpcResult<Container> {
    let create_bucket_req = CreateBucketRequest {
        bucket: container_id.to_string(),
        ..Default::default()
    };
    client
        .create_bucket(create_bucket_req)
        .await
        .map_err(|_e| RpcError::Other("create_bucket() failed".to_string()))
        .unwrap();
    Ok(Container {
        id: container_id.clone(),
    })
}

async fn remove_bucket(
    container_id: String,
    client: &S3Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let delete_bucket_req = DeleteBucketRequest {
        bucket: container_id.to_owned(),
        ..Default::default()
    };

    client.delete_bucket(delete_bucket_req).await?;
    Ok(())
}

async fn list_objects(
    container_id: String,
    client: &S3Client,
) -> Result<Option<Vec<Object>>, Box<dyn std::error::Error + Sync + Send>> {
    let list_obj_req = ListObjectsV2Request {
        bucket: container_id.to_owned(),
        ..Default::default()
    };
    let res: ListObjectsV2Output = client.list_objects_v2(list_obj_req).await?;
    Ok(res.contents)
}

fn build_empty_fileblob() -> FileBlob {
    return FileBlob {
        byte_size: 0,
        container: build_empty_container(),
        id: String::from(""),
    };
}

fn build_empty_container() -> Container {
    return Container {
        id: String::from(""),
    };
}

fn build_empty_blobstore_result() -> BlobstoreResult {
    return BlobstoreResult {
        error: None,
        success: true,
    };
}

/*
/// calculate n factorial
fn n_factorial(n: u32) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        _ => {
            let mut result = 1u64;
            // add 1 because rust ranges exclude upper bound
            for v in 2..(n + 1) {
                result *= v as u64;
            }
            result
        }
    }
}
*/

/// Handle incoming rpc messages and dispatch to applicable trait handler.
#[async_trait]
impl MessageDispatch for BlobstoreS3ProviderProvider {
    async fn dispatch(&self, ctx: &Context, message: Message<'_>) -> RpcResult<Message<'_>> {
        let op = match message.method.split_once('.') {
            Some((cls, op)) if cls == "Blobstore" => op,
            None => message.method,
            _ => {
                return Err(RpcError::MethodNotHandled(message.method.to_string()));
            }
        };
        BlobstoreReceiver::dispatch(
            self,
            ctx,
            &Message {
                method: op,
                arg: message.arg,
            },
        )
        .await
    }
}

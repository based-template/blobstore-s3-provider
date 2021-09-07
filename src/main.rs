//! blobstore-s3-provider capability provider
//!
//!
use blobstore_interface::{
    BlobList, Blobstore, BlobstoreReceiver, BlobstoreResult, Container, FileBlob, FileChunk,
    GetObjectInfoRequest, RemoveObjectRequest, StartDownloadRequest,
};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use wasmbus_rpc::provider::prelude::*;
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
struct BlobstoreS3ProviderProvider {}

/// use default implementations of provider message handlers
impl ProviderDispatch for BlobstoreS3ProviderProvider {}
impl BlobstoreReceiver for BlobstoreS3ProviderProvider {}
impl ProviderHandler for BlobstoreS3ProviderProvider {}

/// Handle Blobstore methods
#[async_trait]
impl Blobstore for BlobstoreS3ProviderProvider {
    /// creates a new container
    async fn create_container<TS: ToString + ?Sized + std::marker::Sync>(
        &self,
        _ctx: &Context,
        arg: &TS,
    ) -> RpcResult<Container> {
        let _container_id = arg.to_string();
        Ok(build_empty_container())
    }

    /// RemoveContainer(id: string): BlobstoreResult
    async fn remove_container<TS: ToString + ?Sized + std::marker::Sync>(
        &self,
        _ctx: &Context,
        arg: &TS,
    ) -> RpcResult<BlobstoreResult> {
        let _container_id = arg.to_string();
        Ok(build_empty_blobstore_result())
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
        _ctx: &Context,
        arg: &TS,
    ) -> RpcResult<BlobList> {
        let _container_id = arg.to_string();
        Ok(BlobList::new())
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
    /*
    /// accepts a number and calculates its factorial
    async fn calculate(&self, _ctx: &Context, req: &u32) -> RpcResult<u64> {
        Ok(n_factorial(*req))
    }
    */
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

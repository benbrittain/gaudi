pub use build::bazel::remote::execution::v2::capabilities_server::*;
pub use build::bazel::remote::execution::v2::content_addressable_storage_server::*;
pub use build::bazel::remote::execution::v2::execution_server::*;
pub use build::bazel::remote::execution::v2::*;
pub use google::longrunning;

mod google {
    pub mod longrunning {
        tonic::include_proto!("google.longrunning");
    }
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
}

pub mod build {
    pub mod bazel {
        pub mod semver {
            tonic::include_proto!("build.bazel.semver");
        }
        pub mod remote {
            pub mod execution {
                pub mod v2 {
                    tonic::include_proto!("build.bazel.remote.execution.v2");
                }
            }
        }
    }
}

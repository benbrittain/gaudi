fn main() {
    println!("Hello, world!");
}

pub mod api {
    mod google {
        pub mod longrunning {
            tonic::include_proto!("google.longrunning");
        }
        pub mod rpc {
            tonic::include_proto!("google.rpc");
        }
    }
    mod build {
        mod bazel {
            mod semver {
                tonic::include_proto!("build.bazel.semver");
            }
            mod remote {
                mod execution {
                    mod v2 {
                        tonic::include_proto!("build.bazel.remote.execution.v2");
                    }
                }
            }
        }
    }
}

fn main() {
    println!("Hello, world!");
}

//pub mod google {
//    #[path = ""]
//    pub mod rpc{
//        #[path = "google.api"]
//        pub mod rpc;
//    }
//}

pub mod api {
//    //mod google {
//    //    mod rpc {
//    //        tonic::include_proto!("google.rpc");
//    //    }
//    //}
    mod google {
        mod rpc {
            tonic::include_proto!("google.api");
        }
    }
    mod build {
        mod bazel {
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

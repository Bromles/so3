mod generated {
    #![allow(
        clippy::default_trait_access,
        clippy::doc_markdown,
        clippy::missing_errors_doc,
        clippy::must_use_candidate,
        clippy::too_many_lines
    )]

    tonic::include_proto!("consensus");
}

pub use generated::*;

pub mod mappers;

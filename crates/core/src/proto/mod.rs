mod generated {
    #![allow(
        clippy::default_trait_access,
        clippy::doc_markdown,
        clippy::missing_errors_doc,
        clippy::must_use_candidate,
        clippy::too_many_lines
    )]
    pub mod consensus {
        tonic::include_proto!("consensus");
    }

    pub mod blob {
        tonic::include_proto!("blob");
    }
}

pub use generated::*;

pub mod mappers;

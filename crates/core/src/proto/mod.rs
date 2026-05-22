mod generated {
    #![allow(
        clippy::default_trait_access,
        clippy::doc_markdown,
        clippy::missing_errors_doc,
        clippy::must_use_candidate,
        clippy::too_many_lines
    )]
    pub mod base {
        tonic::include_proto!("base");
    }

    pub mod consensus {
        tonic::include_proto!("consensus");
    }

    pub mod blob {
        tonic::include_proto!("blob");
    }

    pub mod metadata_query {
        tonic::include_proto!("metadata_query");
    }
}

pub use generated::*;

pub mod mappers;
pub mod metadata_query_mappers;

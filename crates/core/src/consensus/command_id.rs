use crate::domain::error::{So3Error, So3Result};
use crate::rpc_server::proto::CommandId;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConsensusCommandId {
    origin_node_id: String,
    sequence: u64,
}

impl ConsensusCommandId {
    #[must_use]
    pub fn new(origin_node_id: String, sequence: u64) -> Self {
        Self {
            origin_node_id,
            sequence,
        }
    }

    #[must_use]
    pub fn origin_node_id(&self) -> &str {
        &self.origin_node_id
    }

    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl TryFrom<CommandId> for ConsensusCommandId {
    type Error = So3Error;

    fn try_from(value: CommandId) -> So3Result<Self> {
        Self::try_from(&value)
    }
}

impl TryFrom<&CommandId> for ConsensusCommandId {
    type Error = So3Error;

    fn try_from(value: &CommandId) -> So3Result<Self> {
        if value.origin_node_id.trim().is_empty() {
            return Err(So3Error::InvalidRequest(
                "consensus command origin_node_id must not be empty".to_owned(),
            ));
        }

        Ok(Self::new(value.origin_node_id.clone(), value.sequence))
    }
}

#[cfg(test)]
mod tests {
    use crate::consensus::ConsensusCommandId;
    use crate::domain::error::So3Error;
    use crate::rpc_server::proto::CommandId;

    const ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE: u64 = 7;
    const BLANK_ORIGIN_NODE_ID: &str = "   ";

    #[test]
    fn command_id_parses_valid_proto_value() {
        let command_id = ConsensusCommandId::try_from(&CommandId {
            origin_node_id: ORIGIN_NODE_ID.to_owned(),
            sequence: COMMAND_SEQUENCE,
        })
        .unwrap();

        assert_eq!(command_id.origin_node_id(), ORIGIN_NODE_ID);
        assert_eq!(command_id.sequence(), COMMAND_SEQUENCE);
    }

    #[test]
    fn command_id_rejects_blank_origin() {
        let error = ConsensusCommandId::try_from(&CommandId {
            origin_node_id: BLANK_ORIGIN_NODE_ID.to_owned(),
            sequence: COMMAND_SEQUENCE,
        })
        .unwrap_err();

        assert!(matches!(error, So3Error::InvalidRequest(_)));
    }
}

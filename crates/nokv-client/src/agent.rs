pub use nokv_agent::{
    agent_tool_definitions, execute_agent_tool, AgentError, AgentNamespace, AgentToolDefinition,
};

use nokv_meta::{
    NamespaceAggregateRequest, NamespaceAggregateResult, NamespaceCard, NamespaceFindRequest,
    NamespaceFindResult, NamespaceGrepRequest, NamespaceGrepResult, NamespaceListOptions,
    NamespaceListPage, NamespaceReadOptions, NamespaceReadPage,
};
use nokv_object::ObjectStore;

use crate::{ClientError, MetadataClient, NoKvFsClient};

impl From<ClientError> for AgentError {
    fn from(err: ClientError) -> Self {
        match err {
            ClientError::Metadata(e) => AgentError::Metadata(e),
            ClientError::NotFound(p) => AgentError::NotFound(p),
            ClientError::Protocol(s) => AgentError::InvalidArgument(s),
            other => AgentError::Other(other.to_string()),
        }
    }
}

impl AgentNamespace for MetadataClient {
    fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, AgentError> {
        self.stat_card(path).map_err(AgentError::from)
    }

    fn agent_list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, AgentError> {
        self.namespace_list_page(path, options)
            .map_err(AgentError::from)
    }

    fn agent_find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, AgentError> {
        self.find_paths(request).map_err(AgentError::from)
    }

    fn agent_aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, AgentError> {
        self.aggregate_paths(request).map_err(AgentError::from)
    }

    fn agent_grep_paths(
        &self,
        request: NamespaceGrepRequest,
    ) -> Result<NamespaceGrepResult, AgentError> {
        self.grep_paths(request).map_err(AgentError::from)
    }

    fn agent_read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, AgentError> {
        self.read_page(path, options).map_err(AgentError::from)
    }
}

impl<O> AgentNamespace for NoKvFsClient<O>
where
    O: ObjectStore + Send + Sync + 'static,
{
    fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, AgentError> {
        self.metadata().stat_card(path).map_err(AgentError::from)
    }

    fn agent_list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, AgentError> {
        self.metadata()
            .namespace_list_page(path, options)
            .map_err(AgentError::from)
    }

    fn agent_find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, AgentError> {
        self.metadata()
            .find_paths(request)
            .map_err(AgentError::from)
    }

    fn agent_aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, AgentError> {
        self.metadata()
            .aggregate_paths(request)
            .map_err(AgentError::from)
    }

    fn agent_grep_paths(
        &self,
        request: NamespaceGrepRequest,
    ) -> Result<NamespaceGrepResult, AgentError> {
        self.metadata()
            .grep_paths(request)
            .map_err(AgentError::from)
    }

    fn agent_read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, AgentError> {
        self.metadata()
            .read_page(path, options)
            .map_err(AgentError::from)
    }
}

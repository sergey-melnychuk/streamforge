//! Pipeline construction and execution
//!
//! Pipelines define the full data flow from sources to sinks,
//! with operators in between.

/// A pipeline builder for constructing stream processing workflows
pub struct Pipeline {
    name: String,
}

impl Pipeline {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_creation() {
        let pipeline = Pipeline::new("test-pipeline");
        assert_eq!(pipeline.name(), "test-pipeline");
    }
}

//! Query executor
//!
//! Executes queries against streams

use crate::core::{Event, EventKey, EventValue};
use crate::distributed::node::NodeId;
use crate::execution::{DistributedContext, ShuffleManager, Stream};
use crate::network::RpcClient;
use crate::operators::{SessionWindow, SlidingWindow, TumblingWindow, WindowAssigner, join::{JoinType as OperatorJoinType, JoinState, JoinedEvent}};
use crate::query::ast::*;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Query execution result
pub type QueryResult = std::result::Result<(), QueryExecutionError>;

/// Query execution error
#[derive(Debug, thiserror::Error)]
pub enum QueryExecutionError {
    #[error("Execution error: {0}")]
    Execution(String),
    #[error("Stream not found: {0}")]
    StreamNotFound(String),
    #[error("Query error: {0}")]
    Query(String),
}

/// Query executor that translates SQL queries to stream operations
pub struct QueryExecutor {
    pub(crate) query: Query,
    /// Distributed context (optional - if None, executes locally)
    distributed_context: Option<Arc<DistributedContext>>,
    /// Shuffle manager for distributed operations (optional)
    shuffle_manager: Option<Arc<ShuffleManager>>,
    /// RPC client for remote execution (optional)
    rpc_client: Option<Arc<RpcClient>>,
}

impl QueryExecutor {
    /// Create a new query executor (local execution only)
    pub fn new(query: Query) -> Self {
        Self {
            query,
            distributed_context: None,
            shuffle_manager: None,
            rpc_client: None,
        }
    }

    /// Create a new distributed query executor
    pub fn new_distributed(
        query: Query,
        distributed_context: Arc<DistributedContext>,
        shuffle_manager: Arc<ShuffleManager>,
        rpc_client: Arc<RpcClient>,
    ) -> Self {
        Self {
            query,
            distributed_context: Some(distributed_context),
            shuffle_manager: Some(shuffle_manager),
            rpc_client: Some(rpc_client),
        }
    }

    /// Check if this executor is running in distributed mode
    fn is_distributed(&self) -> bool {
        self.distributed_context.is_some()
    }

    /// Execute the query against a stream and return a new stream
    pub async fn execute_stream(&self, input_stream: Stream) -> std::result::Result<Stream, QueryExecutionError> {
        info!("Executing query on stream: {}", self.query.from.primary_stream());

        let mut stream = input_stream;

        // Step 1: Apply WHERE filter
        if let Some(ref where_clause) = self.query.where_clause {
            let condition = where_clause.condition.clone();
            stream = stream.filter(move |event| {
                match condition.evaluate(event) {
                    Value::Boolean(b) => b,
                    _ => false,
                }
            });
        }

        // Step 2: Apply windowing if specified
        if let Some(ref window_spec) = self.query.window {
            debug!("Applying windowing: {:?}", window_spec);
            
            // For streaming windowed aggregations, we need to process events incrementally
            // For now, we still collect events but this can be optimized to true streaming
            let events: Vec<Event> = stream.collect().await
                .map_err(|e| QueryExecutionError::Execution(format!("Failed to collect events: {}", e)))?;
            
            // Apply windowing - distributed if enabled
            let windowed_events = if self.is_distributed() {
                self.apply_distributed_windowed_aggregations(events, window_spec).await?
            } else {
                // Use streaming windowed aggregations for better performance
                self.apply_streaming_windowed_aggregations(events, window_spec).await?
            };
            
            stream = Stream::from_iter(windowed_events);
        } else if self.query.aggregations.is_some() || self.query.group_by.is_some() {
            // Aggregations without windowing require batch processing
            debug!("Aggregations require batch processing");
            // This will be handled by execute_with_aggregations in the caller
        }

        // Step 3: Apply GROUP BY and aggregations (if not already handled by windowing)
        if self.query.window.is_none() {
            if let (Some(_group_by), Some(_aggregations)) = (&self.query.group_by, &self.query.aggregations) {
                // Group by requires collecting events
                debug!("GROUP BY with aggregations requires batch processing");
            }
        }

        // Step 4: Apply SELECT projection
        stream = self.apply_projection(stream)?;

        Ok(stream)
    }

    /// Apply SELECT projection to stream
    fn apply_projection(&self, stream: Stream) -> std::result::Result<Stream, QueryExecutionError> {
        let select_fields = &self.query.select.fields;
        
        // If SELECT *, return stream as-is
        if select_fields.len() == 1 {
            if let SelectField::All = &select_fields[0] {
                return Ok(stream);
            }
        }

        // Otherwise, project fields
        let fields = select_fields.clone();
        let projected = stream.map(move |event| {
            Self::project_event(&event, &fields)
        });

        Ok(projected)
    }

    /// Project event according to SELECT fields
    fn project_event(event: &Event, fields: &[SelectField]) -> Event {
        let mut result_json = serde_json::Map::new();

        for field in fields {
            match field {
                SelectField::All => {
                    // Include all fields from original event
                    if let EventValue::Json(json) = &event.value {
                        if let Some(obj) = json.as_object() {
                            for (k, v) in obj {
                                result_json.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }
                SelectField::Field(name) => {
                    // Extract field from event
                    if let EventValue::Json(json) = &event.value {
                        if let Some(value) = json.get(name) {
                            result_json.insert(name.clone(), value.clone());
                        }
                    }
                }
                SelectField::Aliased { field, alias } => {
                    // Extract field and rename
                    if let EventValue::Json(json) = &event.value {
                        if let Some(value) = json.get(field) {
                            result_json.insert(alias.clone(), value.clone());
                        }
                    }
                }
                SelectField::Aggregation(_agg) => {
                    // Aggregations are handled separately in windowed/grouped queries
                    // For now, skip (would need accumulator state)
                    debug!("Aggregation in projection not yet supported in streaming mode");
                }
            }
        }

        // Add timestamp
        result_json.insert("timestamp".to_string(), json!(event.timestamp));

        Event::new(
            event.key.clone(),
            EventValue::Json(json!(result_json)),
            event.timestamp,
        )
    }

    /// Execute query with aggregations (batch mode)
    pub async fn execute_with_aggregations(
        &self,
        events: Vec<Event>,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        // Filter events (can be done locally on each node)
        let filtered: Vec<Event> = events
            .into_iter()
            .filter(|e| self.matches_filter(e))
            .collect();

        // If distributed and has GROUP BY, execute distributed aggregation
        if self.is_distributed() {
            if let (Some(ref group_by), Some(ref aggregations)) = (&self.query.group_by, &self.query.aggregations) {
                return self.execute_distributed_grouped_aggregations(filtered, group_by, aggregations).await;
            }
            
            // Global aggregations in distributed mode need to collect from all nodes
            if let Some(ref aggregations) = &self.query.aggregations {
                return self.execute_distributed_global_aggregations(filtered, aggregations).await;
            }
        }

        // Local execution path
        // Apply GROUP BY and aggregations
        if let (Some(ref group_by), Some(ref aggregations)) = (&self.query.group_by, &self.query.aggregations) {
            return self.execute_grouped_aggregations(filtered, group_by, aggregations).await;
        }

        // Apply aggregations without GROUP BY
        if let Some(ref aggregations) = &self.query.aggregations {
            return self.execute_global_aggregations(filtered, aggregations).await;
        }

        // No aggregations, just project
        Ok(filtered
            .into_iter()
            .map(|e| self.project(&e))
            .collect())
    }

    /// Apply distributed windowed aggregations
    async fn apply_distributed_windowed_aggregations(
        &self,
        events: Vec<Event>,
        window_spec: &WindowSpec,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        let ctx = self.distributed_context.as_ref().ok_or_else(|| {
            QueryExecutionError::Execution("Distributed context required for distributed windowing".to_string())
        })?;

        info!("Executing distributed windowed aggregation");

        // For distributed windowing:
        // 1. Partition events by window and key (if GROUP BY)
        // 2. Shuffle events to nodes responsible for each window/key combination
        // 3. Aggregate locally on each node
        // 4. Collect and merge results

        // Extract GROUP BY key if present
        let group_by = self.query.group_by.as_deref().unwrap_or(&[]);

        // Partition events by window and key
        let mut events_by_partition: HashMap<u32, Vec<Event>> = HashMap::new();

        for event in events {
            // Determine partition based on GROUP BY key (if any) or event key
            let partition = if !group_by.is_empty() {
                let group_key = self.extract_group_key(&event, group_by);
                self.hash_group_key(&group_key, ctx.num_partitions)
            } else {
                ctx.get_partition_for_event(&event)
            };
            
            events_by_partition
                .entry(partition)
                .or_default()
                .push(event);
        }

        // Shuffle events to appropriate nodes and aggregate
        let mut local_events = Vec::new();
        let mut remote_partitions: HashMap<NodeId, Vec<(u32, Vec<Event>)>> = HashMap::new();

        for (partition, partition_events) in events_by_partition {
            if ctx.is_local_partition(partition).await {
                local_events.extend(partition_events);
            } else if let Some(node_id) = ctx.get_node_for_partition(partition).await {
                remote_partitions
                    .entry(node_id)
                    .or_default()
                    .push((partition, partition_events));
            } else {
                local_events.extend(partition_events);
            }
        }

        // Aggregate locally
        let local_results = if !local_events.is_empty() {
            self.apply_windowed_aggregations(local_events, window_spec).await?
        } else {
            Vec::new()
        };

        // Shuffle remote events (similar to GROUP BY)
        if let Some(shuffle_mgr) = &self.shuffle_manager {
            if let Some(_rpc_client) = &self.rpc_client {
                for (node_id, partitions_data) in remote_partitions {
                    if let Some(node_addr) = ctx.membership.get_node_address(node_id) {
                        for (partition, events) in partitions_data {
                            let events_len = events.len();
                            if let Err(e) = shuffle_mgr.shuffle_to_node(
                                events,
                                node_id,
                                partition,
                                "windowed_aggregation".to_string(),
                                node_addr,
                            ).await {
                                warn!("Failed to shuffle windowed events to node {}: {}", node_id, e);
                            } else {
                                debug!("Shuffled {} windowed events to node {} for partition {}", 
                                    events_len, node_id, partition);
                            }
                        }
                    }
                }
            }
        }

        // Collect and merge results from all nodes
        let all_results = self.collect_and_merge_results(local_results, ctx).await?;
        
        Ok(all_results)
    }

    /// Apply streaming windowed aggregations (incremental processing with watermarks)
    async fn apply_streaming_windowed_aggregations(
        &self,
        events: Vec<Event>,
        window_spec: &WindowSpec,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        use crate::execution::streaming_windowed::StreamingWindowConfig;
        
        // Get aggregations
        let aggregations = self.query.aggregations.as_ref()
            .ok_or_else(|| QueryExecutionError::Query("Windowed aggregations require aggregation functions".to_string()))?;
        
        if aggregations.len() != 1 {
            // For now, only support single aggregation
            return self.apply_windowed_aggregations(events, window_spec).await;
        }
        
        let agg = &aggregations[0];
        let config = StreamingWindowConfig::default();
        
        // Match on both window type and aggregation type to create the right aggregator
        match window_spec.window_type {
            WindowType::Tumbling => {
                match window_spec.size {
                    WindowSize::Time(secs) => {
                        let assigner = TumblingWindow::of(Duration::from_secs(secs));
                        self.execute_streaming_windowed_with_agg(events, assigner, agg, config).await
                    }
                    WindowSize::Count(_) => {
                        Err(QueryExecutionError::Execution(
                            "Count-based windows not yet supported".to_string()
                        ))
                    }
                }
            }
            WindowType::Sliding => {
                match window_spec.size {
                    WindowSize::Time(secs) => {
                        let slide = Duration::from_secs(secs / 2);
                        let assigner = SlidingWindow::of(Duration::from_secs(secs), slide);
                        self.execute_streaming_windowed_with_agg(events, assigner, agg, config).await
                    }
                    WindowSize::Count(_) => {
                        Err(QueryExecutionError::Execution(
                            "Count-based windows not yet supported".to_string()
                        ))
                    }
                }
            }
            WindowType::Session => {
                match window_spec.size {
                    WindowSize::Time(secs) => {
                        let assigner = SessionWindow::with_gap(Duration::from_secs(secs));
                        self.execute_streaming_windowed_with_agg(events, assigner, agg, config).await
                    }
                    WindowSize::Count(_) => {
                        Err(QueryExecutionError::Execution(
                            "Count-based windows not yet supported".to_string()
                        ))
                    }
                }
            }
        }
    }
    
    /// Execute streaming windowed aggregation with specific window assigner and aggregation type
    async fn execute_streaming_windowed_with_agg<W: WindowAssigner + 'static>(
        &self,
        events: Vec<Event>,
        assigner: W,
        agg: &Aggregation,
        config: crate::execution::streaming_windowed::StreamingWindowConfig,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        use crate::execution::streaming_windowed::{FieldAwareAgg, StreamingWindowedAggregator};
        use crate::operators::{Avg, Count, Max, Min, Sum};
        
        // Create the appropriate aggregation function based on type
        match agg.function.to_uppercase().as_str() {
            "COUNT" => {
                let count = Count::new();
                let agg_fn = FieldAwareAgg::new(count, agg.field.clone());
                let aggregator = StreamingWindowedAggregator::new(assigner, agg_fn, config);
                self.process_streaming_aggregator(aggregator, events, agg).await
            }
            "SUM" => {
                let sum = Sum::new();
                let agg_fn = FieldAwareAgg::new(sum, agg.field.clone());
                let aggregator = StreamingWindowedAggregator::new(assigner, agg_fn, config);
                self.process_streaming_aggregator(aggregator, events, agg).await
            }
            "AVG" => {
                let avg = Avg::new();
                let agg_fn = FieldAwareAgg::new(avg, agg.field.clone());
                let aggregator = StreamingWindowedAggregator::new(assigner, agg_fn, config);
                self.process_streaming_aggregator(aggregator, events, agg).await
            }
            "MIN" => {
                let min = Min::new();
                let agg_fn = FieldAwareAgg::new(min, agg.field.clone());
                let aggregator = StreamingWindowedAggregator::new(assigner, agg_fn, config);
                self.process_streaming_aggregator(aggregator, events, agg).await
            }
            "MAX" => {
                let max = Max::new();
                let agg_fn = FieldAwareAgg::new(max, agg.field.clone());
                let aggregator = StreamingWindowedAggregator::new(assigner, agg_fn, config);
                self.process_streaming_aggregator(aggregator, events, agg).await
            }
            _ => {
                Err(QueryExecutionError::Execution(
                    format!("Unsupported aggregation function: {}", agg.function)
                ))
            }
        }
    }
    
    /// Process events through a streaming aggregator and return results
    async fn process_streaming_aggregator<W: WindowAssigner + 'static, A: crate::operators::AggregateFunction + 'static>(
        &self,
        aggregator: crate::execution::streaming_windowed::StreamingWindowedAggregator<W, A>,
        events: Vec<Event>,
        agg: &Aggregation,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        
        // Process events incrementally
        for event in &events {
            aggregator.process_event(event).await
                .map_err(|e| QueryExecutionError::Execution(format!("Streaming aggregation error: {}", e)))?;
        }

        // Get triggered results
        let mut results = aggregator.get_triggered_results().await;
        
        // Trigger any remaining open windows (for final results)
        let final_results = aggregator.trigger_all_windows().await
            .map_err(|e| QueryExecutionError::Execution(format!("Failed to trigger final windows: {}", e)))?;
        results.extend(final_results);

        // Apply projection if needed (add aggregation alias)
        if let Some(ref alias) = agg.alias {
            for result in &mut results {
                if let EventValue::Json(ref mut json) = result.value {
                    if let Some(obj) = json.as_object_mut() {
                        // Rename "value" to alias if it exists
                        if let Some(value) = obj.remove("value") {
                            obj.insert(alias.clone(), value);
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Apply windowed aggregations (local) - batch mode
    async fn apply_windowed_aggregations(
        &self,
        events: Vec<Event>,
        window_spec: &WindowSpec,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        use crate::execution::WindowedStream;
        
        // Create windowed stream based on window type
        match window_spec.window_type {
            WindowType::Tumbling => {
                match window_spec.size {
                    WindowSize::Time(secs) => {
                        let assigner = TumblingWindow::of(Duration::from_secs(secs));
                        let windowed = WindowedStream::new(events, assigner);
                        self.execute_windowed_aggregation(windowed).await
                    }
                    WindowSize::Count(_) => {
                        Err(QueryExecutionError::Execution(
                            "Count-based windows not yet supported".to_string()
                        ))
                    }
                }
            }
            WindowType::Sliding => {
                match window_spec.size {
                    WindowSize::Time(secs) => {
                        // For sliding windows, slide is typically half the size
                        let slide = Duration::from_secs(secs / 2);
                        let assigner = SlidingWindow::of(Duration::from_secs(secs), slide);
                        let windowed = WindowedStream::new(events, assigner);
                        self.execute_windowed_aggregation(windowed).await
                    }
                    WindowSize::Count(_) => {
                        Err(QueryExecutionError::Execution(
                            "Count-based windows not yet supported".to_string()
                        ))
                    }
                }
            }
            WindowType::Session => {
                match window_spec.size {
                    WindowSize::Time(secs) => {
                        let assigner = SessionWindow::with_gap(Duration::from_secs(secs));
                        let windowed = WindowedStream::new(events, assigner);
                        self.execute_windowed_aggregation(windowed).await
                    }
                    WindowSize::Count(_) => {
                        Err(QueryExecutionError::Execution(
                            "Count-based windows not yet supported".to_string()
                        ))
                    }
                }
            }
        }
    }

    /// Execute aggregation on a windowed stream
    async fn execute_windowed_aggregation<W: WindowAssigner + 'static>(
        &self,
        windowed: crate::execution::WindowedStream<W>,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {

        // Apply aggregations if specified
        if let Some(ref aggregations) = self.query.aggregations {
            if aggregations.len() == 1 {
                let agg = &aggregations[0];
                match agg.function.to_uppercase().as_str() {
                    "COUNT" => {
                        let results = windowed.count().await
                            .map_err(|e| QueryExecutionError::Execution(format!("Windowed count failed: {}", e)))?;
                        return Ok(self.window_results_to_events(results, agg));
                    }
                    "SUM" => {
                        if let Some(ref field) = agg.field {
                            let results = windowed.sum().await
                                .map_err(|e| QueryExecutionError::Execution(format!("Windowed sum failed: {}", e)))?;
                            return Ok(self.window_sum_results_to_events(results, agg, field));
                        }
                    }
                    "AVG" => {
                        if let Some(ref field) = agg.field {
                            let results = windowed.avg().await
                                .map_err(|e| QueryExecutionError::Execution(format!("Windowed avg failed: {}", e)))?;
                            return Ok(self.window_avg_results_to_events(results, agg, field));
                        }
                    }
                    "MIN" => {
                        if let Some(ref field) = agg.field {
                            let results = windowed.min().await
                                .map_err(|e| QueryExecutionError::Execution(format!("Windowed min failed: {}", e)))?;
                            return Ok(self.window_minmax_results_to_events(results, agg, field));
                        }
                    }
                    "MAX" => {
                        if let Some(ref field) = agg.field {
                            let results = windowed.max().await
                                .map_err(|e| QueryExecutionError::Execution(format!("Windowed max failed: {}", e)))?;
                            return Ok(self.window_minmax_results_to_events(results, agg, field));
                        }
                    }
                    _ => {}
                }
            }
        }

        // Default: just return windowed events with count
        let count_op = crate::operators::Count;
        let results = windowed.aggregate_to_events(count_op).await
            .map_err(|e| QueryExecutionError::Execution(format!("Windowed aggregation failed: {}", e)))?;
        Ok(results)
    }

    /// Convert window count results to events
    fn window_results_to_events(
        &self,
        results: Vec<(crate::operators::Window, EventKey, i64)>,
        agg: &Aggregation,
    ) -> Vec<Event> {
        results.into_iter().map(|(window, key, count)| {
            let mut json = serde_json::Map::new();
            let field_name = agg.alias.as_ref()
                .unwrap_or(&agg.function.to_lowercase())
                .clone();
            json.insert(field_name, json!(count));
            json.insert("window_start".to_string(), json!(window.start));
            json.insert("window_end".to_string(), json!(window.end));
            Event::new(key, EventValue::Json(json!(json)), window.end)
        }).collect()
    }

    /// Convert window sum results to events
    fn window_sum_results_to_events(
        &self,
        results: Vec<(crate::operators::Window, EventKey, f64)>,
        agg: &Aggregation,
        _field: &str,
    ) -> Vec<Event> {
        results.into_iter().map(|(window, key, sum)| {
            let mut json = serde_json::Map::new();
            let field_name = agg.alias.as_ref()
                .unwrap_or(&agg.function.to_lowercase())
                .clone();
            json.insert(field_name, json!(sum));
            json.insert("window_start".to_string(), json!(window.start));
            json.insert("window_end".to_string(), json!(window.end));
            Event::new(key, EventValue::Json(json!(json)), window.end)
        }).collect()
    }

    /// Convert window avg results to events
    fn window_avg_results_to_events(
        &self,
        results: Vec<(crate::operators::Window, EventKey, Option<f64>)>,
        agg: &Aggregation,
        _field: &str,
    ) -> Vec<Event> {
        results.into_iter().map(|(window, key, avg)| {
            let mut json = serde_json::Map::new();
            let field_name = agg.alias.as_ref()
                .unwrap_or(&agg.function.to_lowercase())
                .clone();
            json.insert(field_name, json!(avg));
            json.insert("window_start".to_string(), json!(window.start));
            json.insert("window_end".to_string(), json!(window.end));
            Event::new(key, EventValue::Json(json!(json)), window.end)
        }).collect()
    }

    /// Convert window min/max results to events
    fn window_minmax_results_to_events(
        &self,
        results: Vec<(crate::operators::Window, EventKey, Option<f64>)>,
        agg: &Aggregation,
        _field: &str,
    ) -> Vec<Event> {
        results.into_iter().map(|(window, key, value)| {
            let mut json = serde_json::Map::new();
            let field_name = agg.alias.as_ref()
                .unwrap_or(&agg.function.to_lowercase())
                .clone();
            json.insert(field_name, json!(value));
            json.insert("window_start".to_string(), json!(window.start));
            json.insert("window_end".to_string(), json!(window.end));
            Event::new(key, EventValue::Json(json!(json)), window.end)
        }).collect()
    }

    /// Execute grouped aggregations
    pub(crate) async fn execute_grouped_aggregations(
        &self,
        events: Vec<Event>,
        group_by: &[String],
        aggregations: &[Aggregation],
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        // Group events by GROUP BY fields
        let mut groups: HashMap<Vec<String>, Vec<Event>> = HashMap::new();

        for event in events {
            let key: Vec<String> = group_by
                .iter()
                .filter_map(|field| {
                    if let EventValue::Json(json) = &event.value {
                        json.get(field).and_then(|v| v.as_str().map(|s| s.to_string()))
                    } else {
                        None
                    }
                })
                .collect();

            groups.entry(key).or_default().push(event);
        }

        // Apply aggregations to each group
        let mut results = Vec::new();
        for (group_key, group_events) in groups {
            let mut result_json = serde_json::Map::new();

            // Add group by fields
            for (i, field) in group_by.iter().enumerate() {
                if let Some(value) = group_key.get(i) {
                    result_json.insert(field.clone(), json!(value));
                }
            }

            // Apply aggregations
            for agg in aggregations {
                let value = self.compute_aggregation(agg, &group_events)?;
                let field_name = agg.alias.as_ref()
                    .unwrap_or(&agg.function.to_lowercase())
                    .clone();
                result_json.insert(field_name, value);
            }

            let result_event = Event::new(
                EventKey::default(),
                EventValue::Json(json!(result_json)),
                chrono::Utc::now().timestamp_millis(),
            );
            results.push(result_event);
        }

        // Apply HAVING clause if present
        if let Some(ref having) = self.query.having {
            results.retain(|e| {
                match having.condition.evaluate(e) {
                    Value::Boolean(b) => b,
                    _ => false,
                }
            });
        }

        // Apply ORDER BY if present
        if let Some(ref order_by) = self.query.order_by {
            results.sort_by(|a, b| {
                for field in &order_by.fields {
                    let a_val = Self::get_field_value(a, &field.field);
                    let b_val = Self::get_field_value(b, &field.field);
                    let cmp = match (a_val, b_val) {
                        (Value::Integer(ai), Value::Integer(bi)) => ai.cmp(&bi),
                        (Value::Float(af), Value::Float(bf)) => af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal),
                        (Value::String(as_), Value::String(bs)) => as_.cmp(&bs),
                        _ => std::cmp::Ordering::Equal,
                    };
                    let cmp = match field.direction {
                        SortDirection::Asc => cmp,
                        SortDirection::Desc => cmp.reverse(),
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply LIMIT and OFFSET
        let start = self.query.offset.unwrap_or(0);
        let end = self.query.limit.map(|l| start + l).unwrap_or(results.len());
        results = results.into_iter().skip(start).take(end - start).collect();

        Ok(results)
    }

    /// Execute global aggregations (no GROUP BY)
    pub(crate) async fn execute_global_aggregations(
        &self,
        events: Vec<Event>,
        aggregations: &[Aggregation],
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        let mut result_json = serde_json::Map::new();

        for agg in aggregations {
            let value = self.compute_aggregation(agg, &events)?;
            let field_name = agg.alias.as_ref()
                .unwrap_or(&agg.function.to_lowercase())
                .clone();
            result_json.insert(field_name, value);
        }

        let result_event = Event::new(
            EventKey::default(),
            EventValue::Json(json!(result_json)),
            chrono::Utc::now().timestamp_millis(),
        );

        let mut results = vec![result_event];

        // Apply HAVING clause if present
        if let Some(ref having) = self.query.having {
            results.retain(|e| {
                    match having.condition.evaluate(e) {
                        Value::Boolean(b) => b,
                        _ => false,
                    }
                });
        }

        // Apply ORDER BY if present (usually not needed for single result, but support it)
        if let Some(ref order_by) = self.query.order_by {
            results.sort_by(|a, b| {
                for field in &order_by.fields {
                    let a_val = Self::get_field_value(a, &field.field);
                    let b_val = Self::get_field_value(b, &field.field);
                    let cmp = match (a_val, b_val) {
                        (Value::Integer(ai), Value::Integer(bi)) => ai.cmp(&bi),
                        (Value::Float(af), Value::Float(bf)) => af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal),
                        (Value::String(as_), Value::String(bs)) => as_.cmp(&bs),
                        _ => std::cmp::Ordering::Equal,
                    };
                    let cmp = match field.direction {
                        SortDirection::Asc => cmp,
                        SortDirection::Desc => cmp.reverse(),
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply LIMIT and OFFSET
        let start = self.query.offset.unwrap_or(0);
        let end = self.query.limit.map(|l| start + l).unwrap_or(results.len());
        results = results.into_iter().skip(start).take(end - start).collect();

        Ok(results)
    }

    /// Get field value from event
    fn get_field_value(event: &Event, field: &str) -> Value {
        if let EventValue::Json(json) = &event.value {
            if let Some(v) = json.get(field) {
                if let Some(s) = v.as_str() {
                    Value::String(s.to_string())
                } else if let Some(i) = v.as_i64() {
                    Value::Integer(i)
                } else if let Some(f) = v.as_f64() {
                    Value::Float(f)
                } else if let Some(b) = v.as_bool() {
                    Value::Boolean(b)
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        } else {
            Value::Null
        }
    }

    /// Compute aggregation value
    fn compute_aggregation(
        &self,
        agg: &Aggregation,
        events: &[Event],
    ) -> std::result::Result<serde_json::Value, QueryExecutionError> {
        let func_name = agg.function.to_uppercase();
        let field = agg.field.as_ref();

        match func_name.as_str() {
            "COUNT" => {
                let count = if field.is_none() || field == Some(&"*".to_string()) {
                    events.len()
                } else {
                    events.iter()
                        .filter(|e| {
                            if let EventValue::Json(json) = &e.value {
                                json.get(field.unwrap()).is_some()
                            } else {
                                false
                            }
                        })
                        .count()
                };
                Ok(json!(count))
            }
            "SUM" => {
                let field = field.ok_or_else(|| QueryExecutionError::Query("SUM requires field name".to_string()))?;
                let sum: f64 = events.iter()
                    .filter_map(|e| {
                        if let EventValue::Json(json) = &e.value {
                            json.get(field)
                                .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                        } else {
                            None
                        }
                    })
                    .sum();
                Ok(json!(sum))
            }
            "AVG" => {
                let field = field.ok_or_else(|| QueryExecutionError::Query("AVG requires field name".to_string()))?;
                let values: Vec<f64> = events.iter()
                    .filter_map(|e| {
                        if let EventValue::Json(json) = &e.value {
                            json.get(field)
                                .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                        } else {
                            None
                        }
                    })
                    .collect();
                let avg = if values.is_empty() {
                    0.0
                } else {
                    values.iter().sum::<f64>() / values.len() as f64
                };
                Ok(json!(avg))
            }
            "MIN" => {
                let field = field.ok_or_else(|| QueryExecutionError::Query("MIN requires field name".to_string()))?;
                let min = events.iter()
                    .filter_map(|e| {
                        if let EventValue::Json(json) = &e.value {
                            json.get(field)
                                .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                        } else {
                            None
                        }
                    })
                    .fold(f64::INFINITY, |a, b| a.min(b));
                Ok(if min == f64::INFINITY { json!(null) } else { json!(min) })
            }
            "MAX" => {
                let field = field.ok_or_else(|| QueryExecutionError::Query("MAX requires field name".to_string()))?;
                let max = events.iter()
                    .filter_map(|e| {
                        if let EventValue::Json(json) = &e.value {
                            json.get(field)
                                .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                        } else {
                            None
                        }
                    })
                    .fold(f64::NEG_INFINITY, |a, b| a.max(b));
                Ok(if max == f64::NEG_INFINITY { json!(null) } else { json!(max) })
            }
            "MEDIAN" => {
                let field = field.ok_or_else(|| QueryExecutionError::Query("MEDIAN requires field name".to_string()))?;
                let mut values: Vec<f64> = events.iter()
                    .filter_map(|e| {
                        if let EventValue::Json(json) = &e.value {
                            json.get(field)
                                .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                        } else {
                            None
                        }
                    })
                    .collect();
                
                if values.is_empty() {
                    Ok(json!(null))
                } else {
                    // Sort values to find median
                    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let len = values.len();
                    let median = if len.is_multiple_of(2) {
                        // Even number of values: average of two middle values
                        (values[len / 2 - 1] + values[len / 2]) / 2.0
                    } else {
                        // Odd number of values: middle value
                        values[len / 2]
                    };
                    Ok(json!(median))
                }
            }
            _ => Err(QueryExecutionError::Query(format!("Unknown aggregation function: {}", func_name))),
        }
    }

    /// Check if event matches WHERE clause
    pub fn matches_filter(&self, event: &Event) -> bool {
        if let Some(ref where_clause) = self.query.where_clause {
            match where_clause.condition.evaluate(event) {
                Value::Boolean(b) => b,
                _ => false,
            }
        } else {
            true
        }
    }

    /// Project event according to SELECT clause
    pub fn project(&self, event: &Event) -> Event {
        Self::project_event(event, &self.query.select.fields)
    }

    /// Execute distributed grouped aggregations
    /// Partitions events by GROUP BY key, shuffles to appropriate nodes, aggregates locally, then merges
    async fn execute_distributed_grouped_aggregations(
        &self,
        events: Vec<Event>,
        group_by: &[String],
        aggregations: &[Aggregation],
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        let ctx = self.distributed_context.as_ref().ok_or_else(|| {
            QueryExecutionError::Execution("Distributed context required for distributed execution".to_string())
        })?;
        let _shuffle_mgr = self.shuffle_manager.as_ref().ok_or_else(|| {
            QueryExecutionError::Execution("Shuffle manager required for distributed execution".to_string())
        })?;

        info!("Executing distributed GROUP BY aggregation across {} partitions", ctx.num_partitions);

        // Step 1: Partition events by GROUP BY key hash
        let mut events_by_partition: HashMap<u32, Vec<Event>> = HashMap::new();

        for event in events {
            // Extract GROUP BY key from event
            let group_key = self.extract_group_key(&event, group_by);
            
            // Hash the group key to determine partition
            let partition = self.hash_group_key(&group_key, ctx.num_partitions);
            
            events_by_partition
                .entry(partition)
                .or_default()
                .push(event);
        }

        // Step 2: Shuffle events to appropriate nodes
        let mut local_events = Vec::new();
        let mut remote_partitions: HashMap<NodeId, Vec<(u32, Vec<Event>)>> = HashMap::new();

        for (partition, partition_events) in events_by_partition {
            if ctx.is_local_partition(partition).await {
                // Process locally
                local_events.extend(partition_events);
            } else {
                // Shuffle to remote node
                if let Some(node_id) = ctx.get_node_for_partition(partition).await {
                    remote_partitions
                        .entry(node_id)
                        .or_default()
                        .push((partition, partition_events));
                } else {
                    warn!("No node found for partition {}, processing locally", partition);
                    local_events.extend(partition_events);
                }
            }
        }

        // Step 3: Aggregate locally
        let mut local_results = if !local_events.is_empty() {
            self.execute_grouped_aggregations(local_events, group_by, aggregations).await?
        } else {
            Vec::new()
        };

        // Step 4: Execute query aggregation remotely using DistributedExecutor pattern
        if let Some(rpc_client) = &self.rpc_client {
            for (node_id, partitions_data) in remote_partitions {
                if let Some(node_addr) = ctx.membership.get_node_address(node_id) {
                    // For each partition, execute query aggregation remotely
                    for (partition, events) in partitions_data {
                        let _events_len = events.len();
                        // Clone events for fallback processing if remote execution fails
                        let events_clone = events.clone();
                        
                        // Serialize query for remote execution
                        let query_serialized = bincode::serialize(&self.query)
                            .map_err(|e| QueryExecutionError::Execution(format!("Failed to serialize query: {}", e)))?;
                        
                        // Create request for remote query aggregation execution
                        let request = crate::network::protocol::ExecuteQueryAggregationRequest {
                            query_id: format!("query_{}_{}", ctx.local_node_id, partition),
                            query: query_serialized,
                            events: events.clone(),
                            partition,
                        };
                        
                        let payload = bincode::serialize(&request)
                            .map_err(|e| QueryExecutionError::Execution(format!("Serialization error: {}", e)))?;
                        
                        // Execute query aggregation remotely via RPC
                        match rpc_client.call(node_addr, crate::network::protocol::RpcMethod::ExecuteQueryAggregation.as_str(), payload).await {
                            Ok(response_payload) => {
                                match bincode::deserialize::<crate::network::protocol::ExecuteQueryAggregationResponse>(&response_payload) {
                                    Ok(response) => {
                                        debug!("Executed query aggregation remotely on node {} for partition {}: {} results", 
                                            node_id, partition, response.results.len());
                                        // Store remote results for later merging
                                        // Note: In a full implementation, we'd store these and merge after collecting from all nodes
                                        // For now, we'll add them to local_results for merging
                                        local_results.extend(response.results);
                                    }
                                    Err(e) => {
                                        warn!("Failed to deserialize query aggregation response from node {}: {}", node_id, e);
                                        // Fallback: process locally
                                        let fallback_results = self.execute_grouped_aggregations(events_clone, group_by, aggregations).await?;
                                        local_results.extend(fallback_results);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to execute query aggregation remotely on node {}: {}, processing locally as fallback", node_id, e);
                                // Fallback: process locally if remote execution fails (e.g., node unavailable)
                                let fallback_results = self.execute_grouped_aggregations(events_clone, group_by, aggregations).await?;
                                local_results.extend(fallback_results);
                            }
                        }
                    }
                }
            }
        }

        // Step 5: Collect and merge results from all nodes
        let all_results = self.collect_and_merge_results(local_results, ctx).await?;
        
        // Apply HAVING, ORDER BY, LIMIT/OFFSET
        self.apply_post_aggregation_clauses(all_results).await
    }

    /// Execute distributed global aggregations
    async fn execute_distributed_global_aggregations(
        &self,
        events: Vec<Event>,
        aggregations: &[Aggregation],
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        // For global aggregations, we need to:
        // 1. Aggregate locally on each node
        // 2. Shuffle partial results to a coordinator node
        // 3. Merge partial results into final result
        
        let _ctx = self.distributed_context.as_ref().ok_or_else(|| {
            QueryExecutionError::Execution("Distributed context required for distributed execution".to_string())
        })?;

        info!("Executing distributed global aggregation");

        // Aggregate locally first
        let local_results = self.execute_global_aggregations(events, aggregations).await?;

        // In a full implementation, we'd:
        // 1. Send local results to coordinator (typically node 0 or a designated coordinator)
        // 2. Coordinator merges all partial results
        // 3. Return final result
        
        // For now, return local results (in production, this would be merged from all nodes)
        self.apply_post_aggregation_clauses(local_results).await
    }

    /// Extract GROUP BY key from event
    fn extract_group_key(&self, event: &Event, group_by: &[String]) -> Vec<String> {
        group_by
            .iter()
            .filter_map(|field| {
                if let EventValue::Json(json) = &event.value {
                    json.get(field).and_then(|v| v.as_str().map(|s| s.to_string()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Hash GROUP BY key to determine partition
    fn hash_group_key(&self, group_key: &[String], num_partitions: u32) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        for key_part in group_key {
            key_part.hash(&mut hasher);
        }
        (hasher.finish() as u32) % num_partitions
    }

    /// Apply HAVING, ORDER BY, LIMIT/OFFSET to results
    async fn apply_post_aggregation_clauses(
        &self,
        mut results: Vec<Event>,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        // Apply HAVING clause if present
        if let Some(ref having) = self.query.having {
            results.retain(|e| {
                match having.condition.evaluate(e) {
                    Value::Boolean(b) => b,
                    _ => false,
                }
            });
        }

        // Apply ORDER BY if present
        if let Some(ref order_by) = self.query.order_by {
            results.sort_by(|a, b| {
                for field in &order_by.fields {
                    let a_val = Self::get_field_value(a, &field.field);
                    let b_val = Self::get_field_value(b, &field.field);
                    let cmp = match (a_val, b_val) {
                        (Value::Integer(ai), Value::Integer(bi)) => ai.cmp(&bi),
                        (Value::Float(af), Value::Float(bf)) => af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal),
                        (Value::String(as_), Value::String(bs)) => as_.cmp(&bs),
                        _ => std::cmp::Ordering::Equal,
                    };
                    let cmp = match field.direction {
                        SortDirection::Asc => cmp,
                        SortDirection::Desc => cmp.reverse(),
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply LIMIT and OFFSET
        let start = self.query.offset.unwrap_or(0);
        let end = self.query.limit.map(|l| start + l).unwrap_or(results.len());
        results = results.into_iter().skip(start).take(end - start).collect();

        Ok(results)
    }

    /// Collect and merge results from all nodes for GROUP BY aggregations
    async fn collect_and_merge_results(
        &self,
        local_results: Vec<Event>,
        ctx: &DistributedContext,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        // If we're the coordinator (typically the first node or local node), collect from all nodes
        // For simplicity, we'll use the local node as coordinator
        let is_coordinator = true; // In production, you'd designate a specific coordinator
        
        if !is_coordinator {
            // If not coordinator, just return local results
            // In a full implementation, we'd send results to coordinator
            return Ok(local_results);
        }

        // Coordinator: collect results from all nodes
        let mut all_results = local_results;
        
        if let Some(rpc_client) = &self.rpc_client {
            let nodes = ctx.membership.get_alive_nodes();
            let query_id = format!("query_{}", ctx.local_node_id); // Simple query ID
            
            for node_metadata in nodes {
                let node_id = node_metadata.id;
                if node_id == ctx.local_node_id {
                    continue; // Skip self
                }
                
                if let Some(node_addr) = ctx.membership.get_node_address(node_id) {
                    // Request results from this node
                    let request = crate::network::protocol::CollectQueryResultsRequest {
                        query_id: query_id.clone(),
                    };
                    
                    let payload = bincode::serialize(&request)
                        .map_err(|e| QueryExecutionError::Execution(format!("Serialization error: {}", e)))?;
                    
                    match rpc_client.call(node_addr, crate::network::protocol::RpcMethod::CollectQueryResults.as_str(), payload).await {
                        Ok(response_payload) => {
                            match bincode::deserialize::<crate::network::protocol::CollectQueryResultsResponse>(&response_payload) {
                                Ok(response) => {
                                    debug!("Collected {} results from node {}", response.results.len(), node_id);
                                    all_results.extend(response.results);
                                }
                                Err(e) => {
                                    warn!("Failed to deserialize results from node {}: {}", node_id, e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to collect results from node {}: {}", node_id, e);
                        }
                    }
                }
            }
        }

        // Merge results by GROUP BY key (if applicable)
        if let Some(ref group_by) = self.query.group_by {
            all_results = self.merge_grouped_results(all_results, group_by).await?;
        }

        Ok(all_results)
    }

    /// Collect and merge global aggregation results from all nodes
    #[allow(unused)]
    async fn collect_and_merge_global_results(
        &self,
        local_results: Vec<Event>,
        ctx: &DistributedContext,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        // Similar to collect_and_merge_results but for global aggregations
        // Global aggregations need to merge partial results (e.g., sum all sums, avg all avgs)
        // For simplicity, we'll use the local node as coordinator
        let is_coordinator = true; // In production, you'd designate a specific coordinator
        
        if !is_coordinator {
            return Ok(local_results);
        }

        let mut all_results = local_results;
        
        if let Some(rpc_client) = &self.rpc_client {
            let nodes = ctx.membership.get_alive_nodes();
            let query_id = format!("query_global_{}", ctx.local_node_id);
            
            for node_metadata in nodes {
                let node_id = node_metadata.id;
                if node_id == ctx.local_node_id {
                    continue;
                }
                
                if let Some(node_addr) = ctx.membership.get_node_address(node_id) {
                    let request = crate::network::protocol::CollectQueryResultsRequest {
                        query_id: query_id.clone(),
                    };
                    
                    let payload = bincode::serialize(&request)
                        .map_err(|e| QueryExecutionError::Execution(format!("Serialization error: {}", e)))?;
                    
                    match rpc_client.call(node_addr, crate::network::protocol::RpcMethod::CollectQueryResults.as_str(), payload).await {
                        Ok(response_payload) => {
                            match bincode::deserialize::<crate::network::protocol::CollectQueryResultsResponse>(&response_payload) {
                                Ok(response) => {
                                    // Merge partial aggregation results
                                    all_results = self.merge_global_aggregation_results(all_results, response.results).await?;
                                }
                                Err(e) => {
                                    warn!("Failed to deserialize global results from node {}: {}", node_id, e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to collect global results from node {}: {}", node_id, e);
                        }
                    }
                }
            }
        }

        Ok(all_results)
    }

    /// Merge grouped results from multiple nodes
    /// Groups with the same key need their aggregations merged
    async fn merge_grouped_results(
        &self,
        results: Vec<Event>,
        group_by: &[String],
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        // Group results by GROUP BY key
        let mut merged_groups: HashMap<Vec<String>, Vec<Event>> = HashMap::new();
        
        for result in results {
            let group_key = self.extract_group_key(&result, group_by);
            merged_groups.entry(group_key).or_default().push(result);
        }

        // For each group, merge aggregations if there are multiple results
        let mut final_results = Vec::new();
        
        if let Some(ref aggregations) = self.query.aggregations {
            for (group_key, group_results) in merged_groups {
                if group_results.len() == 1 {
                    // Single result, no merging needed
                    final_results.push(group_results[0].clone());
                } else {
                    // Multiple results for same group key - need to merge aggregations
                    let merged = self.merge_aggregation_results(group_results, aggregations, &group_key, group_by).await?;
                    final_results.push(merged);
                }
            }
        } else {
            // No aggregations, just deduplicate
            for group_results in merged_groups.values() {
                if let Some(first) = group_results.first() {
                    final_results.push(first.clone());
                }
            }
        }

        Ok(final_results)
    }

    /// Merge aggregation results from multiple nodes for the same group
    async fn merge_aggregation_results(
        &self,
        results: Vec<Event>,
        aggregations: &[Aggregation],
        group_key: &[String],
        group_by: &[String],
    ) -> std::result::Result<Event, QueryExecutionError> {
        use serde_json::json;
        
        let mut merged_json = serde_json::Map::new();
        
        // Add GROUP BY fields
        for (i, field) in group_by.iter().enumerate() {
            if let Some(key_value) = group_key.get(i) {
                merged_json.insert(field.clone(), json!(key_value));
            }
        }
        
        // Merge each aggregation
        for agg in aggregations {
            let func_name = agg.function.to_uppercase();
            let field_name = agg.alias.as_ref()
                .unwrap_or(&agg.function.to_lowercase())
                .clone();
            
            match func_name.as_str() {
                "COUNT" => {
                    // Sum all counts
                    let total: i64 = results.iter()
                        .filter_map(|r| {
                            if let EventValue::Json(json) = &r.value {
                                json.get(&field_name).and_then(|v| v.as_i64())
                            } else {
                                None
                            }
                        })
                        .sum();
                    merged_json.insert(field_name, json!(total));
                }
                "SUM" => {
                    // Sum all sums
                    let total: f64 = results.iter()
                        .filter_map(|r| {
                            if let EventValue::Json(json) = &r.value {
                                json.get(&field_name).and_then(|v| v.as_f64())
                            } else {
                                None
                            }
                        })
                        .sum();
                    merged_json.insert(field_name, json!(total));
                }
                "AVG" => {
                    // Weighted average: need to track count and sum
                    let mut total_sum = 0.0;
                    let mut total_count = 0;
                    
                    for r in &results {
                        if let EventValue::Json(json) = &r.value {
                            if let Some(avg) = json.get(&field_name).and_then(|v| v.as_f64()) {
                                // For AVG, we'd need the count too - simplified for now
                                // In production, we'd track (sum, count) pairs
                                total_sum += avg;
                                total_count += 1;
                            }
                        }
                    }
                    
                    // This is a simplified merge - in production, we'd track sum and count separately
                    let merged_avg = if total_count > 0 { total_sum / total_count as f64 } else { 0.0 };
                    merged_json.insert(field_name, json!(merged_avg));
                }
                "MIN" => {
                    // Minimum of all minimums
                    let min_val = results.iter()
                        .filter_map(|r| {
                            if let EventValue::Json(json) = &r.value {
                                json.get(&field_name).and_then(|v| v.as_f64())
                            } else {
                                None
                            }
                        })
                        .fold(f64::INFINITY, |a, b| a.min(b));
                    if min_val != f64::INFINITY {
                        merged_json.insert(field_name, json!(min_val));
                    }
                }
                "MAX" => {
                    // Maximum of all maximums
                    let max_val = results.iter()
                        .filter_map(|r| {
                            if let EventValue::Json(json) = &r.value {
                                json.get(&field_name).and_then(|v| v.as_f64())
                            } else {
                                None
                            }
                        })
                        .fold(f64::NEG_INFINITY, |a, b| a.max(b));
                    if max_val != f64::NEG_INFINITY {
                        merged_json.insert(field_name, json!(max_val));
                    }
                }
                "MEDIAN" => {
                    // For MEDIAN, we need to collect all values and compute median
                    // This is more complex - collect all values, sort, find median
                    let mut all_values = Vec::new();
                    for r in &results {
                        if let EventValue::Json(json) = &r.value {
                            if let Some(val) = json.get(&field_name).and_then(|v| v.as_f64()) {
                                all_values.push(val);
                            }
                        }
                    }
                    
                    if !all_values.is_empty() {
                        all_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        let len = all_values.len();
                        let median = if len % 2 == 0 {
                            (all_values[len / 2 - 1] + all_values[len / 2]) / 2.0
                        } else {
                            all_values[len / 2]
                        };
                        merged_json.insert(field_name, json!(median));
                    }
                }
                _ => {
                    warn!("Unknown aggregation function for merging: {}", func_name);
                }
            }
        }
        
        Ok(Event::new(
            EventKey::default(),
            EventValue::Json(json!(merged_json)),
            chrono::Utc::now().timestamp_millis(),
        ))
    }

    /// Merge global aggregation results from multiple nodes
    async fn merge_global_aggregation_results(
        &self,
        local_results: Vec<Event>,
        remote_results: Vec<Event>,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        // For global aggregations, we typically have one result per node
        // Merge them into a single result
        if local_results.is_empty() {
            return Ok(remote_results);
        }
        if remote_results.is_empty() {
            return Ok(local_results);
        }
        
        // Combine all results and merge aggregations
        let mut all_results = local_results;
        all_results.extend(remote_results);
        
        // Merge into single result
        if let Some(ref aggregations) = self.query.aggregations {
            if let Some(first) = all_results.first() {
                let mut merged = first.clone();
                
                // Merge with remaining results
                for result in all_results.iter().skip(1) {
                    merged = self.merge_aggregation_results(
                        vec![merged, result.clone()],
                        aggregations,
                        &[],
                        &[],
                    ).await?;
                }
                
                return Ok(vec![merged]);
            }
        }
        
        Ok(all_results)
    }

    /// Execute query with JOINs (batch mode)
    /// Takes left and right stream events and performs the JOIN
    pub async fn execute_with_joins(
        &self,
        left_events: Vec<Event>,
        right_events: Vec<Event>,
    ) -> std::result::Result<Vec<Event>, QueryExecutionError> {
        // Extract JOIN information from FROM clause
        let (join_type, join_condition, left_alias, right_alias) = match &self.query.from {
            FromClause::Join { join_type, condition, right, left } => {
                // Get aliases
                let left_alias = match left.as_ref() {
                    FromClause::Single { alias, .. } => alias.clone(),
                    FromClause::Join { .. } => None, // Nested joins not fully supported yet
                };
                let right_alias = right.alias.clone();
                
                // Convert AST JoinType to operator JoinType
                let op_join_type = match join_type {
                    JoinType::Inner => OperatorJoinType::Inner,
                    JoinType::Left => OperatorJoinType::Left,
                    JoinType::Right => OperatorJoinType::Right,
                    JoinType::Outer => OperatorJoinType::Outer,
                };
                
                (op_join_type, condition, left_alias, right_alias)
            }
            _ => return Err(QueryExecutionError::Query("Not a JOIN query".to_string())),
        };

        // Don't filter events before joining - WHERE clause should be applied after join
        // because it may reference fields from both tables (e.g., "o.amount > 150")
        
        // Extract join keys from events based on JOIN condition
        // The condition is like "left_field = right_field" or "table.field = table.field"
        let left_field = Self::extract_field_name(&join_condition.left_field, &left_alias);
        let right_field = Self::extract_field_name(&join_condition.right_field, &right_alias);

        // Re-key events based on JOIN condition
        let left_rekeyed: Vec<Event> = left_events
            .into_iter()
            .map(|e| Self::rekey_for_join(e, &left_field))
            .collect();
        
        let right_rekeyed: Vec<Event> = right_events
            .into_iter()
            .map(|e| Self::rekey_for_join(e, &right_field))
            .collect();

        // Perform the join using existing join operators
        let mut join_state = JoinState::new();
        for event in left_rekeyed {
            join_state.add_left(event);
        }
        for event in right_rekeyed {
            join_state.add_right(event);
        }

        let joined_events = join_state.join(join_type, None);

        // Convert JoinedEvent to regular Event with combined fields
        let mut results: Vec<Event> = joined_events
            .into_iter()
            .map(|je| Self::joined_event_to_event(je, &left_alias, &right_alias))
            .collect();

        // Apply WHERE filter AFTER join (WHERE clause may reference fields from both tables)
        if let Some(ref where_clause) = self.query.where_clause {
            results.retain(|e| {
                match where_clause.condition.evaluate(e) {
                    Value::Boolean(b) => b,
                    _ => false,
                }
            });
        }

        // Apply SELECT projection
        let projected: Vec<Event> = results
            .into_iter()
            .map(|e| Self::project_event(&e, &self.query.select.fields))
            .collect();

        // Apply HAVING, ORDER BY, LIMIT/OFFSET
        self.apply_post_aggregation_clauses(projected).await
    }

    /// Extract field name from qualified field (table.field or just field)
    fn extract_field_name(qualified_field: &str, _table_alias: &Option<String>) -> String {
        if let Some(dot_pos) = qualified_field.find('.') {
            // Qualified field: table.field
            qualified_field[(dot_pos + 1)..].to_string()
        } else {
            // Unqualified field: just field name
            qualified_field.to_string()
        }
    }

    /// Re-key an event based on a field value for JOIN
    fn rekey_for_join(event: Event, field: &str) -> Event {
        // Extract field value and use it as the key
        let new_key = if let EventValue::Json(json) = &event.value {
            if let Some(value) = json.get(field) {
                // Use field value as key
                if let Some(s) = value.as_str() {
                    EventKey::from_str(s)
                } else if let Some(i) = value.as_i64() {
                    EventKey::from_str(&i.to_string())
                } else {
                    event.key.clone()
                }
            } else {
                event.key.clone()
            }
        } else {
            event.key.clone()
        };

        Event::new(new_key, event.value, event.timestamp)
    }

    /// Convert JoinedEvent to regular Event with combined fields
    fn joined_event_to_event(
        joined: JoinedEvent,
        left_alias: &Option<String>,
        right_alias: &Option<String>,
    ) -> Event {
        let mut result_json = serde_json::Map::new();

        // Add left side fields
        if let Some(left_val) = &joined.left_value {
            if let EventValue::Json(json) = left_val {
                if let Some(obj) = json.as_object() {
                    for (k, v) in obj {
                        let field_name = if let Some(alias) = left_alias {
                            format!("{}.{}", alias, k)
                        } else {
                            k.clone()
                        };
                        result_json.insert(field_name, v.clone());
                    }
                }
            }
        }

        // Add right side fields
        if let Some(right_val) = &joined.right_value {
            if let EventValue::Json(json) = right_val {
                if let Some(obj) = json.as_object() {
                    for (k, v) in obj {
                        let field_name = if let Some(alias) = right_alias {
                            format!("{}.{}", alias, k)
                        } else {
                            k.clone()
                        };
                        result_json.insert(field_name, v.clone());
                    }
                }
            }
        }

        Event::new(
            joined.key,
            EventValue::Json(json!(result_json)),
            joined.timestamp,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::{Event, EventKey, EventValue};
    use serde_json::json;

    #[tokio::test]
    async fn test_query_executor_filter() {
        let query = Query::select("events").where_clause(Expression::binary_op(
            Expression::field("temperature"),
            BinaryOperator::Gt,
            Expression::literal(Value::Integer(20)),
        ));

        let executor = QueryExecutor::new(query);

        let event_data = json!({"temperature": 25});
        let event = Event::new(
            EventKey::from_str("sensor"),
            EventValue::Json(event_data),
            0,
        );

        assert!(executor.matches_filter(&event));

        let event_data = json!({"temperature": 15});
        let event = Event::new(
            EventKey::from_str("sensor"),
            EventValue::Json(event_data),
            0,
        );

        assert!(!executor.matches_filter(&event));
    }

    #[tokio::test]
    async fn test_query_executor_median() {
        use crate::query::SqlParser;
        
        let query = SqlParser::parse("SELECT MEDIAN(temperature) FROM events").unwrap();
        let executor = QueryExecutor::new(query);

        let events = vec![
            Event::new(EventKey::default(), EventValue::Json(json!({"temperature": 10})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"temperature": 20})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"temperature": 30})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"temperature": 40})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"temperature": 50})), 0),
        ];

        let results = executor.execute_with_aggregations(events).await.unwrap();
        assert_eq!(results.len(), 1);
        
        if let EventValue::Json(json) = &results[0].value {
            let median = json.get("median").and_then(|v| v.as_f64());
            assert_eq!(median, Some(30.0)); // Median of [10, 20, 30, 40, 50] is 30
        } else {
            panic!("Expected JSON result");
        }
    }

    #[tokio::test]
    async fn test_query_executor_median_even_count() {
        use crate::query::SqlParser;
        
        let query = SqlParser::parse("SELECT MEDIAN(temperature) FROM events").unwrap();
        let executor = QueryExecutor::new(query);

        let events = vec![
            Event::new(EventKey::default(), EventValue::Json(json!({"temperature": 10})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"temperature": 20})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"temperature": 30})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"temperature": 40})), 0),
        ];

        let results = executor.execute_with_aggregations(events).await.unwrap();
        assert_eq!(results.len(), 1);
        
        if let EventValue::Json(json) = &results[0].value {
            let median = json.get("median").and_then(|v| v.as_f64());
            assert_eq!(median, Some(25.0)); // Median of [10, 20, 30, 40] is (20+30)/2 = 25
        } else {
            panic!("Expected JSON result");
        }
    }

    #[tokio::test]
    async fn test_join_inner() {
        use crate::query::SqlParser;
        
        let query = SqlParser::parse("SELECT * FROM orders o INNER JOIN payments p ON o.id = p.order_id").unwrap();
        let executor = QueryExecutor::new(query);

        let left_events = vec![
            Event::new(EventKey::default(), EventValue::Json(json!({"id": 1, "amount": 100})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"id": 2, "amount": 200})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"id": 3, "amount": 300})), 0),
        ];

        let right_events = vec![
            Event::new(EventKey::default(), EventValue::Json(json!({"order_id": 1, "payment_method": "card"})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"order_id": 2, "payment_method": "cash"})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"order_id": 4, "payment_method": "card"})), 0), // No matching order
        ];

        let results = executor.execute_with_joins(left_events, right_events).await.unwrap();
        
        // Should have 2 results (orders 1 and 2 match)
        assert_eq!(results.len(), 2);
        
        // Check first result (order 1)
        if let EventValue::Json(json) = &results[0].value {
            assert_eq!(json.get("o.id"), Some(&json!(1)));
            assert_eq!(json.get("p.order_id"), Some(&json!(1)));
        } else {
            panic!("Expected JSON result");
        }
    }

    #[tokio::test]
    async fn test_join_left() {
        use crate::query::SqlParser;
        
        let query = SqlParser::parse("SELECT * FROM orders LEFT JOIN payments ON orders.id = payments.order_id").unwrap();
        let executor = QueryExecutor::new(query);

        let left_events = vec![
            Event::new(EventKey::default(), EventValue::Json(json!({"id": 1, "amount": 100})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"id": 2, "amount": 200})), 0),
        ];

        let right_events = vec![
            Event::new(EventKey::default(), EventValue::Json(json!({"order_id": 1, "payment_method": "card"})), 0),
            // Order 2 has no payment
        ];

        let results = executor.execute_with_joins(left_events, right_events).await.unwrap();
        
        // Should have 2 results (both orders, order 2 with null payment)
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_join_with_where() {
        use crate::query::SqlParser;
        
        let query = SqlParser::parse("SELECT * FROM orders o INNER JOIN payments p ON o.id = p.order_id WHERE o.amount > 150").unwrap();
        let executor = QueryExecutor::new(query);

        let left_events = vec![
            Event::new(EventKey::default(), EventValue::Json(json!({"id": 1, "amount": 100})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"id": 2, "amount": 200})), 0),
        ];

        let right_events = vec![
            Event::new(EventKey::default(), EventValue::Json(json!({"order_id": 1, "payment_method": "card"})), 0),
            Event::new(EventKey::default(), EventValue::Json(json!({"order_id": 2, "payment_method": "cash"})), 0),
        ];

        let results = executor.execute_with_joins(left_events, right_events).await.unwrap();
        
        // Should have 1 result (only order 2 has amount > 150)
        assert_eq!(results.len(), 1);
        
        if let EventValue::Json(json) = &results[0].value {
            assert_eq!(json.get("o.id"), Some(&json!(2)));
        } else {
            panic!("Expected JSON result");
        }
    }
}

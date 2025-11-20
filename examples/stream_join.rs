//! Stream join example
//!
//! Demonstrates various join types and temporal joins
//!
//! Run with: cargo run --example stream_join

use std::time::Duration;
use streamforge::core::{Event, EventKey, EventValue};
use streamforge::execution::Stream;
use streamforge::operators::{JoinType, TemporalConstraint};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== StreamForge Stream Join Example ===\n");

    // Example 1: Inner Join - User clicks with user profiles
    println!("1. Inner Join: User Clicks + User Profiles");
    println!("   (Only users with both clicks and profiles)\n");

    let clicks = vec![
        Event::new(
            EventKey::from_str("user1"),
            EventValue::from_str("clicked-ad-1"),
            1000,
        ),
        Event::new(
            EventKey::from_str("user2"),
            EventValue::from_str("clicked-ad-2"),
            2000,
        ),
        Event::new(
            EventKey::from_str("user3"),
            EventValue::from_str("clicked-ad-3"),
            3000,
        ),
    ];

    let profiles = vec![
        Event::new(
            EventKey::from_str("user1"),
            EventValue::from_str("premium"),
            1000,
        ),
        Event::new(
            EventKey::from_str("user2"),
            EventValue::from_str("free"),
            1000,
        ),
        // user3 has no profile
    ];

    let click_stream = Stream::from_iter(clicks);
    let profile_stream = Stream::from_iter(profiles);

    let joined = click_stream
        .join(profile_stream)
        .await?
        .join_type(JoinType::Inner)
        .execute()
        .await?;

    let results = joined.collect().await?;
    println!("   Results: {} joined events", results.len());
    for result in &results {
        println!(
            "      Key: {}, Click: {}, Profile: {}",
            result.key,
            result.left_value.as_ref().unwrap(),
            result.right_value.as_ref().unwrap()
        );
    }
    println!();

    // Example 2: Left Join - Preserve all clicks
    println!("2. Left Join: User Clicks + User Profiles");
    println!("   (All clicks, with null profiles if not found)\n");

    let clicks = vec![
        Event::new(
            EventKey::from_str("user1"),
            EventValue::from_str("clicked-ad-1"),
            1000,
        ),
        Event::new(
            EventKey::from_str("user2"),
            EventValue::from_str("clicked-ad-2"),
            2000,
        ),
        Event::new(
            EventKey::from_str("user3"),
            EventValue::from_str("clicked-ad-3"),
            3000,
        ),
    ];

    let profiles = vec![
        Event::new(
            EventKey::from_str("user1"),
            EventValue::from_str("premium"),
            1000,
        ),
        Event::new(
            EventKey::from_str("user2"),
            EventValue::from_str("free"),
            1000,
        ),
    ];

    let click_stream = Stream::from_iter(clicks);
    let profile_stream = Stream::from_iter(profiles);

    let joined = click_stream
        .join(profile_stream)
        .await?
        .join_type(JoinType::Left)
        .execute()
        .await?;

    let results = joined.collect().await?;
    println!(
        "   Results: {} events (all clicks preserved)",
        results.len()
    );
    for result in &results {
        let profile = result
            .right_value
            .as_ref()
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| "null".to_string());
        println!(
            "      Key: {}, Click: {}, Profile: {}",
            result.key,
            result.left_value.as_ref().unwrap(),
            profile
        );
    }
    println!();

    // Example 3: Temporal Join with time bounds
    println!("3. Temporal Join: Orders + Payments");
    println!("   (Only join if payment within 10 seconds of order)\n");

    let orders = vec![
        Event::new(
            EventKey::from_str("order1"),
            EventValue::from_int(100),
            1000,
        ),
        Event::new(
            EventKey::from_str("order2"),
            EventValue::from_int(200),
            5000,
        ),
        Event::new(
            EventKey::from_str("order3"),
            EventValue::from_int(300),
            10000,
        ),
    ];

    let payments = vec![
        Event::new(
            EventKey::from_str("order1"),
            EventValue::from_int(100),
            2000,
        ), // 1s later - OK
        Event::new(
            EventKey::from_str("order2"),
            EventValue::from_int(200),
            20000,
        ), // 15s later - Too late!
        Event::new(
            EventKey::from_str("order3"),
            EventValue::from_int(300),
            11000,
        ), // 1s later - OK
    ];

    let order_stream = Stream::from_iter(orders);
    let payment_stream = Stream::from_iter(payments);

    let joined = order_stream
        .join(payment_stream)
        .await?
        .with_temporal_constraint(TemporalConstraint::of(Duration::from_secs(10)))
        .execute()
        .await?;

    let results = joined.collect().await?;
    println!("   Results: {} matched pairs (within 10s)", results.len());
    for result in &results {
        println!(
            "      Order: {}, Amount: ${}, Payment: ${}",
            result.key,
            result.left_value.as_ref().unwrap().as_int().unwrap(),
            result.right_value.as_ref().unwrap().as_int().unwrap()
        );
    }
    println!();

    // Example 4: Combining values after join
    println!("4. Combining Values: Temperature Sensors");
    println!("   (Indoor + Outdoor temperatures, compute average)\n");

    let indoor = vec![
        Event::new(EventKey::from_str("room1"), EventValue::from_int(22), 1000),
        Event::new(EventKey::from_str("room2"), EventValue::from_int(24), 1000),
    ];

    let outdoor = vec![
        Event::new(EventKey::from_str("room1"), EventValue::from_int(18), 1000),
        Event::new(EventKey::from_str("room2"), EventValue::from_int(20), 1000),
    ];

    let indoor_stream = Stream::from_iter(indoor);
    let outdoor_stream = Stream::from_iter(outdoor);

    let joined = indoor_stream.join(outdoor_stream).await?.execute().await?;

    // Compute average of indoor and outdoor
    let averaged = joined
        .combine(|left, right| {
            let indoor_temp = left.and_then(|v| v.as_float()).unwrap_or(0.0);
            let outdoor_temp = right.and_then(|v| v.as_float()).unwrap_or(0.0);
            let avg = (indoor_temp + outdoor_temp) / 2.0;
            EventValue::Float(avg)
        })
        .await?;

    println!("   Averaged temperatures:");
    for event in averaged {
        println!(
            "      {}: {:.1}°C",
            event.key,
            event.value.as_float().unwrap()
        );
    }
    println!();

    // Example 5: Multiple matches (Cartesian product)
    println!("5. Multiple Matches: Products + Reviews");
    println!("   (Multiple reviews per product)\n");

    let products = vec![
        Event::new(
            EventKey::from_str("prod1"),
            EventValue::from_str("Laptop"),
            1000,
        ),
        Event::new(
            EventKey::from_str("prod1"),
            EventValue::from_str("Laptop-v2"),
            2000,
        ),
    ];

    let reviews = vec![
        Event::new(
            EventKey::from_str("prod1"),
            EventValue::from_str("Great!"),
            1000,
        ),
        Event::new(
            EventKey::from_str("prod1"),
            EventValue::from_str("Excellent!"),
            2000,
        ),
    ];

    let product_stream = Stream::from_iter(products);
    let review_stream = Stream::from_iter(reviews);

    let joined = product_stream.join(review_stream).await?.execute().await?;

    let results = joined.collect().await?;
    println!(
        "   Results: {} combinations (2 products × 2 reviews)",
        results.len()
    );
    for result in &results {
        println!(
            "      Product: {}, Review: {}",
            result.left_value.as_ref().unwrap(),
            result.right_value.as_ref().unwrap()
        );
    }
    println!();

    println!("=== All join examples completed! ===");
    Ok(())
}

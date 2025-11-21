//! Windowed aggregation example
//!
//! Demonstrates tumbling and sliding windows with various aggregations
//!
//! Run with: cargo run --example windowed_aggregation

use std::time::Duration;
use streamforge::core::{Event, EventKey, EventValue};
use streamforge::execution::Stream;
use streamforge::operators::{SlidingWindow, TumblingWindow};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== StreamForge Windowed Aggregation Example ===\n");

    // Simulated sensor data: temperature readings over time
    let readings = vec![
        (10, 1000),   // 1 second: 10°C
        (12, 2000),   // 2 seconds: 12°C
        (15, 3000),   // 3 seconds: 15°C
        (18, 61000),  // 61 seconds: 18°C
        (20, 62000),  // 62 seconds: 20°C
        (22, 63000),  // 63 seconds: 22°C
        (25, 121000), // 121 seconds: 25°C
    ];

    println!("Sample data: Temperature readings");
    for (temp, ts) in &readings {
        println!("  {}°C at {}ms", temp, ts);
    }
    println!();

    // Example 1: Tumbling Windows (1 minute)
    println!("1. Tumbling Windows (60-second windows):");
    let events: Vec<_> = readings
        .iter()
        .map(|(temp, ts)| {
            Event::new(
                EventKey::from_str("sensor-1"),
                EventValue::from_int(*temp),
                *ts,
            )
        })
        .collect();

    let stream = Stream::from_iter(events);
    let windowed = stream
        .window(TumblingWindow::of(Duration::from_secs(60)))
        .await?;

    let counts = windowed.count().await?;
    for (window, _key, count) in counts {
        println!(
            "  Window [{} - {}): {} events",
            window.start / 1000,
            window.end / 1000,
            count
        );
    }
    println!();

    // Example 2: Calculate average per window
    println!("2. Average temperature per window:");
    let events: Vec<_> = readings
        .iter()
        .map(|(temp, ts)| {
            Event::new(
                EventKey::from_str("sensor-1"),
                EventValue::from_int(*temp),
                *ts,
            )
        })
        .collect();

    let stream = Stream::from_iter(events);
    let windowed = stream
        .window(TumblingWindow::of(Duration::from_secs(60)))
        .await?;

    let averages = windowed.avg().await?;
    for (window, _, avg) in averages {
        println!(
            "  Window [{:3}s - {:3}s): {:5.1}°C",
            window.start / 1000,
            window.end / 1000,
            avg.unwrap_or(0.0)
        );
    }
    println!();

    // Example 3: Min/Max per window
    println!("3. Min/Max temperature per window:");
    let events: Vec<_> = readings
        .iter()
        .map(|(temp, ts)| {
            Event::new(
                EventKey::from_str("sensor-1"),
                EventValue::from_int(*temp),
                *ts,
            )
        })
        .collect();

    let stream_min = Stream::from_iter(events.clone());
    let windowed_min = stream_min
        .window(TumblingWindow::of(Duration::from_secs(60)))
        .await?;
    let mins = windowed_min.min().await?;

    let stream_max = Stream::from_iter(events);
    let windowed_max = stream_max
        .window(TumblingWindow::of(Duration::from_secs(60)))
        .await?;
    let maxs = windowed_max.max().await?;

    for ((win_min, _, min), (_, _, max)) in mins.iter().zip(maxs.iter()) {
        println!(
            "  Window [{:3}s - {:3}s): min={:5.1}°C, max={:5.1}°C",
            win_min.start / 1000,
            win_min.end / 1000,
            min.unwrap_or(0.0),
            max.unwrap_or(0.0)
        );
    }
    println!();

    // Example 4: Sliding Windows (30-second windows, 15-second slide)
    println!("4. Sliding Windows (30s windows, sliding every 15s):");
    let events: Vec<_> = vec![(10, 1000), (12, 16000), (15, 31000), (18, 46000)]
        .iter()
        .map(|(temp, ts)| {
            Event::new(
                EventKey::from_str("sensor-1"),
                EventValue::from_int(*temp),
                *ts,
            )
        })
        .collect();

    let stream = Stream::from_iter(events);
    let windowed = stream
        .window(SlidingWindow::of(
            Duration::from_secs(30),
            Duration::from_secs(15),
        ))
        .await?;

    let results = windowed.avg().await?;
    for (window, _, avg) in results {
        println!(
            "  Window [{:3}s - {:3}s): {:5.1}°C",
            window.start / 1000,
            window.end / 1000,
            avg.unwrap_or(0.0)
        );
    }
    println!();

    // Example 5: Multiple keys (sensors)
    println!("5. Multiple sensors with windowing:");
    let multi_sensor_data = vec![
        ("sensor-1", 20, 1000),
        ("sensor-2", 25, 1000),
        ("sensor-1", 22, 2000),
        ("sensor-2", 27, 2000),
        ("sensor-1", 21, 3000),
        ("sensor-2", 26, 3000),
    ];

    let events: Vec<_> = multi_sensor_data
        .iter()
        .map(|(sensor, temp, ts)| {
            Event::new(
                EventKey::from_str(*sensor),
                EventValue::from_int(*temp),
                *ts,
            )
        })
        .collect();

    let stream = Stream::from_iter(events);
    let windowed = stream
        .window(TumblingWindow::of(Duration::from_secs(60)))
        .await?;

    let sensor_avgs = windowed.avg().await?;
    for (window, key, avg) in sensor_avgs {
        println!(
            "  {}: {:5.1}°C (window {}s - {}s)",
            key,
            avg.unwrap_or(0.0),
            window.start / 1000,
            window.end / 1000
        );
    }
    println!();

    println!("=== All windowing examples completed! ===");
    Ok(())
}

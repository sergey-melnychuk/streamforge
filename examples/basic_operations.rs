//! Basic stream operations example
//!
//! Demonstrates filter, map, and chaining
//!
//! Run with: cargo run --example basic_operations

use streamforge::execution::Stream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== StreamForge Basic Operations Example ===\n");

    // Example 1: Simple filtering
    println!("1. Filter even numbers:");
    let stream = Stream::from_values(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let evens = stream
        .filter(|e| e.value.as_int().unwrap_or(0) % 2 == 0)
        .collect()
        .await?;

    print!("   Input:  [1..10]\n   Output: [");
    for (i, event) in evens.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", event.value.as_int().unwrap());
    }
    println!("]\n");

    // Example 2: Mapping values
    println!("2. Double each number:");
    let stream = Stream::from_values(vec![1, 2, 3, 4, 5]);
    let doubled = stream
        .map(|e| {
            let val = e.value.as_int().unwrap_or(0) * 2;
            e.with_value_changed(val.into())
        })
        .collect()
        .await?;

    print!("   Input:  [1, 2, 3, 4, 5]\n   Output: [");
    for (i, event) in doubled.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", event.value.as_int().unwrap());
    }
    println!("]\n");

    // Example 3: Chaining operations
    println!("3. Chain: filter > map > filter:");
    let stream = Stream::from_values(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result = stream
        .filter(|e| e.value.as_int().unwrap_or(0) > 3) // Keep > 3
        .map(|e| {
            let val = e.value.as_int().unwrap_or(0) * 3; // Multiply by 3
            e.with_value_changed(val.into())
        })
        .filter(|e| e.value.as_int().unwrap_or(0) < 25) // Keep < 25
        .collect()
        .await?;

    print!("   Input:  [1..10]\n   After filter (>3):  [4..10]\n   After map (*3):     [12, 15, 18, 21, 24, 27, 30]\n   After filter (<25): [");
    for (i, event) in result.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", event.value.as_int().unwrap());
    }
    println!("]\n");

    // Example 4: Aggregations
    println!("4. Stream aggregations:");
    let count = Stream::from_values(vec![1, 2, 3, 4, 5]).count().await?;
    let sum = Stream::from_values(vec![1, 2, 3, 4, 5])
        .fold(0, |acc, e| acc + e.value.as_int().unwrap_or(0))
        .await?;

    println!("   Count: {}", count);
    println!("   Sum:   {}\n", sum);

    // Example 5: FlatMap
    println!("5. FlatMap - expand each number into a range:");
    let stream = Stream::from_values(vec![2, 3, 1]);
    let expanded = stream
        .flat_map(|e| {
            let n = e.value.as_int().unwrap_or(0);
            (0..n)
                .map(|i| streamforge::core::Event::with_value(i.into()))
                .collect()
        })
        .collect()
        .await?;

    print!("   Input:  [2, 3, 1]\n   Output: [");
    for (i, event) in expanded.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", event.value.as_int().unwrap());
    }
    println!("] (expanded: 0,1 + 0,1,2 + 0)\n");

    println!("=== All examples completed successfully! ===");

    Ok(())
}

//! Throughput benchmarks for stream processing

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use streamforge::core::{Event, EventKey, EventValue};
use streamforge::execution::Stream;

async fn process_stream(n: usize) {
    let events: Vec<_> = (0..n)
        .map(|i| {
            Event::new(
                EventKey::from_int(i as i64),
                EventValue::from_int(i as i64),
                i as i64,
            )
        })
        .collect();

    let _ = Stream::from_iter(events)
        .filter(|e| e.value.as_int().unwrap_or(0) % 2 == 0)
        .map(|e| {
            let val = e.value.as_int().unwrap_or(0) * 2;
            e.with_value_changed(EventValue::from_int(val))
        })
        .collect()
        .await
        .unwrap();
}

fn throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");

    for size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            b.to_async(rt)
                .iter(|| process_stream(std::hint::black_box(size)));
        });
    }

    group.finish();
}

fn filter_only_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_only");

    for size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            b.to_async(rt).iter(|| async {
                let events: Vec<_> = (0..size)
                    .map(|i| Event::new(EventKey::None, EventValue::from_int(i as i64), 0))
                    .collect();

                let _ = Stream::from_iter(events)
                    .filter(|e| e.value.as_int().unwrap_or(0) % 2 == 0)
                    .collect()
                    .await
                    .unwrap();
            });
        });
    }

    group.finish();
}

fn map_only_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_only");

    for size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            b.to_async(rt).iter(|| async {
                let events: Vec<_> = (0..size)
                    .map(|i| Event::new(EventKey::None, EventValue::from_int(i as i64), 0))
                    .collect();

                let _ = Stream::from_iter(events)
                    .map(|e| {
                        let val = e.value.as_int().unwrap_or(0) * 2;
                        e.with_value_changed(EventValue::from_int(val))
                    })
                    .collect()
                    .await
                    .unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    throughput_benchmark,
    filter_only_benchmark,
    map_only_benchmark
);
criterion_main!(benches);

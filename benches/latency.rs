//! Latency benchmarks for individual event processing

use criterion::{criterion_group, criterion_main, Criterion};
use streamforge::core::{Event, EventKey, EventValue};
use streamforge::operators::{FilterOp, MapOp, StreamOperator};

fn single_event_filter(c: &mut Criterion) {
    c.bench_function("single_event_filter", |b| {
        let mut filter = FilterOp::new(|e: &Event| e.value.as_int().unwrap_or(0) > 50);
        let event = Event::new(EventKey::None, EventValue::from_int(100), 0);

        b.iter(|| {
            let _ = filter.process(std::hint::black_box(event.clone())).unwrap();
        });
    });
}

fn single_event_map(c: &mut Criterion) {
    c.bench_function("single_event_map", |b| {
        let mut mapper = MapOp::new(|e: Event| {
            let val = e.value.as_int().unwrap_or(0) * 2;
            e.with_value_changed(EventValue::from_int(val))
        });
        let event = Event::new(EventKey::None, EventValue::from_int(42), 0);

        b.iter(|| {
            let _ = mapper.process(std::hint::black_box(event.clone())).unwrap();
        });
    });
}

fn event_creation(c: &mut Criterion) {
    c.bench_function("event_creation", |b| {
        b.iter(|| {
            let _ = Event::new(
                std::hint::black_box(EventKey::from_int(123)),
                std::hint::black_box(EventValue::from_int(456)),
                std::hint::black_box(1000),
            );
        });
    });
}

fn event_clone(c: &mut Criterion) {
    let event = Event::new(EventKey::from_str("key"), EventValue::from_int(123), 1000);

    c.bench_function("event_clone", |b| {
        b.iter(|| {
            let _ = std::hint::black_box(event.clone());
        });
    });
}

criterion_group!(
    benches,
    single_event_filter,
    single_event_map,
    event_creation,
    event_clone
);
criterion_main!(benches);

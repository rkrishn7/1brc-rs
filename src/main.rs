use memmap::MmapOptions;
use std::collections::HashMap;
use std::fs::File;

fn main() {
    let file = File::open("./data/measurements.txt").unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };

    let mut stats = Stats::new();

    for line in mmap.split(|c| *c == b'\n') {
        let delim = line.iter().position(|c| *c == b';');

        if let Some(delim) = delim {
            let (city, reading) = line.split_at(delim);

            let city = std::str::from_utf8(city).unwrap();
            let reading = std::str::from_utf8(&reading[1..]).unwrap();
            let reading: f64 = reading.parse().unwrap();

            stats.update(city, reading);
        }
    }

    let mut stats = stats.inner.into_iter().collect::<Vec<_>>();
    stats.sort_unstable_by_key(|i| i.0);

    for (city, stats) in stats {
        println!("{}: {}/{}/{}", city, stats.min, stats.avg, stats.max);
    }
}

#[derive(Debug)]
struct Stats<'a> {
    inner: HashMap<&'a str, StatsPer>,
}

impl<'a> Stats<'a> {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn update(&mut self, city: &'a str, reading: f64) {
        match self.inner.entry(city) {
            std::collections::hash_map::Entry::Occupied(ref mut entry) => {
                entry.get_mut().push(reading);
            }
            std::collections::hash_map::Entry::Vacant(vacant) => {
                let mut stats = StatsPer::new();
                stats.push(reading);
                vacant.insert(stats);
            }
        }
    }
}

#[derive(Debug)]
struct StatsPer {
    min: f64,
    max: f64,
    avg: f64,
    seen: usize,
}

impl StatsPer {
    pub fn new() -> Self {
        Self {
            min: 0.0,
            max: 0.0,
            avg: 0.0,
            seen: 0,
        }
    }

    pub fn push(&mut self, reading: f64) {
        if reading < self.min {
            self.min = reading;
        }

        if reading > self.max {
            self.max = reading;
        }

        self.seen += 1;

        let seen = self.seen as f64;

        self.avg = (self.avg * (seen - 1.0) + reading) / seen;
    }
}

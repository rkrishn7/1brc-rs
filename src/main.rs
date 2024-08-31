use fxhash::FxHashMap;
use memmap2::MmapOptions;
use std::fs::File;
use std::i32;
use std::os::unix::fs::MetadataExt;

#[derive(Clone, Copy)]
struct SendMap(*const u8);

unsafe impl Send for SendMap {}

fn main() {
    let file = File::open("./data/measurements.txt").unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    mmap.advise(memmap2::Advice::Sequential).unwrap();

    let metadata = file.metadata().unwrap();
    let size = metadata.size();
    let num_threads = 32;
    let step = size / num_threads;

    let mut results = Vec::new();

    let send_map = SendMap(mmap.as_ptr());

    for i in 0..num_threads {
        results.push(std::thread::spawn(move || {
            let send_map = send_map;
            let mut end_ptr = send_map.0;
            let mut start_ptr = send_map.0;
            let start = i * step;

            if start != 0 {
                unsafe {
                    start_ptr = start_ptr.add(start as usize);
                    while *start_ptr != b'\n' {
                        start_ptr = start_ptr.add(1);
                    }

                    start_ptr = start_ptr.add(1);
                }
            }

            let end = (i + 1) * step;

            unsafe {
                end_ptr = end_ptr.add(end as usize);
                while *end_ptr != b'\n' {
                    end_ptr = end_ptr.add(1);
                }
            }

            let len = end_ptr as usize - start_ptr as usize;

            let mmap = unsafe { std::slice::from_raw_parts(start_ptr, len) };

            let mut stats = Stats::new();

            for line in mmap.split(|c| *c == b'\n') {
                let delim = line.iter().position(|c| *c == b';');

                if let Some(delim) = delim {
                    let (city, reading) = unsafe { line.split_at_unchecked(delim) };

                    // Each reading in `measurements.txt` is guaranteed to be less than
                    // 100 and only have one number after its decimal point.
                    let base = b'0' as i32;
                    let reading = match &reading[1..] {
                        [b'-', a, b'.', c] => -1 * (((*a as i32 % base) * 10) + (*c as i32 % base)),
                        [b'-', a, b, b'.', c] => {
                            -1 * (((*a as i32 % base) * 100)
                                + ((*b as i32 % base) * 10)
                                + (*c as i32 % base))
                        }
                        [a, b'.', c] => ((*a as i32 % base) * 10) + (*c as i32 % base),
                        [a, b, b'.', c] => {
                            ((*a as i32 % base) * 100)
                                + ((*b as i32 % base) * 10)
                                + (*c as i32 % base)
                        }
                        _ => panic!("unrecognized format for reading"),
                    };

                    let city = unsafe { std::str::from_utf8_unchecked(city) };

                    stats.update(city, reading);
                }
            }

            stats
        }));
    }

    let mut stats = Stats::new();

    for handle in results {
        let stats_partial = handle.join().unwrap();
        stats.merge(stats_partial);
    }

    let mut stats = stats.inner.into_iter().collect::<Vec<_>>();
    stats.sort_unstable_by_key(|i| i.0);

    for (city, stats) in stats {
        println!("{}: {}/{}/{}", city, stats.min(), stats.avg(), stats.max());
    }
}

#[derive(Debug)]
struct Stats<'a> {
    inner: FxHashMap<&'a str, StatsPer>,
}

impl<'a> Stats<'a> {
    pub fn new() -> Self {
        Self {
            inner: FxHashMap::default(),
        }
    }

    pub fn update(&mut self, city: &'a str, reading: i32) {
        self.inner
            .entry(city)
            .and_modify(|s| s.push(reading))
            .or_insert({
                let mut stats = StatsPer::new();
                stats.push(reading);
                stats
            });
    }

    pub fn merge(&mut self, other: Stats<'a>) {
        for entry in other.inner.into_iter() {
            match self.inner.entry(entry.0) {
                std::collections::hash_map::Entry::Occupied(ref mut occ) => {
                    occ.get_mut().merge(entry.1);
                }
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    vacant.insert(entry.1);
                }
            }
        }
    }
}

#[derive(Debug)]
struct StatsPer {
    min: i32,
    max: i32,
    sum: i32,
    count: usize,
}

impl StatsPer {
    pub fn new() -> Self {
        Self {
            min: i32::MAX,
            max: i32::MIN,
            sum: 0,
            count: 0,
        }
    }

    pub fn push(&mut self, reading: i32) {
        self.min = std::cmp::min(self.min, reading);
        self.max = std::cmp::max(self.max, reading);

        self.sum += reading;
        self.count += 1;
    }

    pub fn merge(&mut self, other: StatsPer) {
        self.min = std::cmp::min(self.min, other.min);
        self.max = std::cmp::max(self.max, other.max);

        self.sum += other.sum;
        self.count += other.count;
    }

    pub fn avg(&self) -> f32 {
        (self.sum as f32 / self.count as f32) / 10.0
    }

    pub fn min(&self) -> f32 {
        self.min as f32 / 10.0
    }

    pub fn max(&self) -> f32 {
        self.max as f32 / 10.0
    }
}

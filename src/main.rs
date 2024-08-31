use fxhash::FxHashMap;
use memmap2::MmapOptions;
use std::fs::File;
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
    let num_threads = 16;
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

            // let mut temp: [u8; 6] = [0; 6];

            for line in mmap.split(|c| *c == b'\n') {
                let delim = line.iter().position(|c| *c == b';');

                if let Some(delim) = delim {
                    let (city, reading) = unsafe { line.split_at_unchecked(delim) };

                    // unsafe {
                    //     memcpy(
                    //         temp.as_ptr() as *mut c_void,
                    //         reading[1..].as_ptr() as *const c_void,
                    //         reading.len() - 1,
                    //     );
                    // }

                    // temp[reading.len() - 1] = b'\0';

                    let city = unsafe { std::str::from_utf8_unchecked(city) };
                    let reading = unsafe {
                        // libc::atof(temp.as_ptr() as *const i8)
                        std::str::from_utf8_unchecked(&reading[1..])
                            .parse()
                            .unwrap()
                    };

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
        println!("{}: {}/{}/{}", city, stats.min, stats.avg, stats.max);
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

    pub fn update(&mut self, city: &'a str, reading: f64) {
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
        self.min = f64::min(self.min, reading);
        self.max = f64::max(self.max, reading);

        self.seen += 1;

        let seen = self.seen as f64;

        self.avg = (self.avg * (seen - 1.0) + reading) / seen;
    }

    pub fn merge(&mut self, other: StatsPer) {
        self.min = f64::min(self.min, other.min);
        self.max = f64::max(self.max, other.max);

        self.avg = (self.avg + other.avg) / 2.0;
    }
}

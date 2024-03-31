use anyhow::Result;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    fmt::Display,
    path::Path,
    time,
};
use tokio::{
    fs::OpenOptions,
    io::{AsyncBufReadExt, BufReader},
    sync::mpsc::{self, Sender},
};

/// name, value
#[derive(Clone)]
struct Reading(String, f32);

#[derive(Clone)]
struct Stored {
    pub min: f32,
    pub max: f32,
    pub sum: f32,
    pub count: usize,
}

impl Display for Stored {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.1}/{:.1}/{:.1}", self.min, self.mean(), self.max)
    }
}

impl Stored {
    fn new(v: f32) -> Self {
        Self {
            min: v,
            max: v,
            sum: v,
            count: 1,
        }
    }

    fn update(&mut self, v: f32) {
        self.count += 1;
        self.sum += v;
        if v < self.min {
            self.min = v;
        } else if v > self.max {
            self.max = v;
        }
    }

    fn mean(&self) -> f32 {
        self.sum / self.count as f32
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Hello, world!");
    let started = time::SystemTime::now();

    let (tx, mut rx) = mpsc::channel(32);

    tokio::spawn(async move {
        match read_file("./weather_stations.csv", tx).await {
            Ok(ct) => println!("Read {ct} lines."),
            Err(e) => panic!("Something broke: {e}"),
        }
    });

    let mut values: HashMap<String, Stored> = HashMap::new();
    while let Some(this) = rx.recv().await {
        values
            .entry(this.0)
            .and_modify(|current| {
                current.update(this.1);
            })
            .or_insert(Stored::new(this.1));
    }

    let mut sorted: Vec<(String, Stored)> =
        values.into_par_iter().collect::<Vec<(String, Stored)>>();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let l = sorted.len();
    let i = sorted.into_iter().enumerate();

    print!("{{");
    for (idx, (name, nums)) in i {
        print!("{name}={nums}");
        if idx + 1 != l {
            print!(", ");
        }
    }
    println!("}}");

    let dur = time::SystemTime::now().duration_since(started)?.as_micros();

    println!("Runtime: {dur} uSec");

    Ok(())
}

async fn read_file<P>(filename: P, tx: Sender<Reading>) -> Result<usize>
where
    P: AsRef<Path>,
{
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .open(filename)
        .await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        match split_line(line) {
            Ok(Some(r)) => tx.send(r).await?,
            Ok(None) => continue,
            Err(_) => continue,
        }
    }

    Ok(0)
}

fn split_line(line: String) -> Result<Option<Reading>> {
    if line.starts_with('#') {
        return Ok(None);
    }

    if let Some((lhs, rhs)) = line.split_once(';') {
        return Ok(Some(Reading(lhs.to_string(), rhs.parse()?)));
    }

    Ok(None)
}

use clap::Parser;
use comfy_table::{Cell, CellAlignment, Table};
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::{IndexedRandom, SliceRandom};
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;
use std::io::BufWriter;
use std::{
    collections::hash_map::{DefaultHasher, HashMap},
    fs::{self, File},
    hash::{Hash, Hasher},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

/// CLI options.
#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    /// Target directory where files/folders will be created.
    target_path: PathBuf,

    /// Optional random seed (default is 42)
    #[arg(short, long, default_value_t = 42)]
    seed: u64,

    /// Total output size.
    #[arg(long, default_value = "10GB")]
    total: String,

    /// Comma-separated list of candidate file sizes.
    #[arg(long, default_value = "1KB,100KB,10MB")]
    sizes: String,

    /// Exponent α for weighting: weight = (file_size)^(–α).
    #[arg(long, default_value_t = 0.5)]
    alpha: f64,

    /// Minimum number of subdirectories per first-level directory.
    #[arg(long, default_value_t = 2)]
    min_dirs: usize,

    /// Maximum number of subdirectories per first-level directory.
    #[arg(long, default_value_t = 5)]
    max_dirs: usize,
}

/// Constants for folder structure, filenames, and placement weights.
const TOP_LEVEL_FOLDERS: &[&str] = &[
    "Behemoth",
    "Canterbury",
    "Eros",
    "Pella",
    "Rocinante",
    "Razorback",
    "Tycho",
];
const SUBFOLDER_CANDIDATES_ORIG: &[&str] = &[
    "Aberrant",
    "AbernathyStation",
    "AgathaKing",
    "AndersonStation",
    "Arboghast",
    "Artemis",
    "Basalt",
    "Belter",
    "Ceres",
    "Chronicle",
    "Cipher",
    "Ganymede",
    "Kaguya",
    "Kyiv",
    "Laconia",
    "MaoKwikowski",
    "Martian",
    "Nauvoo",
    "Persephone",
    "Phoebe",
    "Plasma",
    "Protogen",
    "Somnambulist",
    "Stardust",
];
const PLACEMENT_WEIGHTS: [f64; 3] = [0.001, 0.05, 0.95];
const FILENAME_WORDS: &[&str] = &[
    "amber",
    "asteroid",
    "basalt",
    "belter",
    "burn",
    "cascade",
    "cipher",
    "chronicle",
    "cinder",
    "cobalt",
    "cosmos",
    "cradle",
    "delta",
    "drift",
    "drone",
    "ejecta",
    "eon",
    "expanse",
    "fusion",
    "gate",
    "horizon",
    "injection",
    "kinetic",
    "knot",
    "laser",
    "lunar",
    "metal",
    "nova",
    "orbital",
    "plasma",
    "proto",
    "quantum",
    "raze",
    "ring",
    "spin",
    "station",
    "system",
    "thrust",
    "torus",
    "vacuum",
    "void",
    "wander",
    "whisper",
    "volt",
    "xenon",
    "zenith",
];
const FILE_EXTENSIONS: &[&str] = &[".txt", ".md", ".log", ".dat", ".cfg", ".bin"];

/// A candidate file size (in bytes).
#[derive(Debug, Clone, Copy)]
struct FileSizeCandidate {
    bytes: u64,
}

/// Folder classification.
#[derive(Debug, Clone)]
enum Folder {
    Root,
    First(String),
    Second { parent: String, name: String },
}

/// A task to create one file.
struct FileTask {
    size_bytes: u64,
    filepath: PathBuf,
}

/// Parse a size string like "1KB", "100KB", "10MB", "10GB" (case‑insensitive) into bytes.
fn parse_size(s: &str) -> u64 {
    let s = s.trim().to_lowercase();
    if s.ends_with("kb") {
        s.strip_suffix("kb")
            .unwrap()
            .trim()
            .parse::<f64>()
            .map(|n| (n * 1024.0) as u64)
            .unwrap()
    } else if s.ends_with("mb") {
        s.strip_suffix("mb")
            .unwrap()
            .trim()
            .parse::<f64>()
            .map(|n| (n * 1024.0 * 1024.0) as u64)
            .unwrap()
    } else if s.ends_with("gb") {
        s.strip_suffix("gb")
            .unwrap()
            .trim()
            .parse::<f64>()
            .map(|n| (n * 1024.0 * 1024.0 * 1024.0) as u64)
            .unwrap()
    } else {
        s.parse::<u64>().unwrap()
    }
}

/// Convert a Folder enum into an actual filesystem path (relative to the target directory).
fn folder_to_path(folder: &Folder, target: &Path) -> PathBuf {
    match folder {
        Folder::Root => target.to_path_buf(),
        Folder::First(name) => target.join(name),
        Folder::Second { parent, name } => target.join(parent).join(name),
    }
}

/// Create a two-level folder structure under the target directory.
/// Returns a vector of Folder items.
fn create_folders_and_return_folders(
    target: &Path,
    rng: &mut ChaCha8Rng,
    min_subfolders: usize,
    max_subfolders: usize,
) -> io::Result<Vec<Folder>> {
    let mut folders = Vec::new();
    // The root folder.
    folders.push(Folder::Root);

    // Create first-level folders.
    for &name in TOP_LEVEL_FOLDERS.iter() {
        let folder = Folder::First(name.to_string());
        fs::create_dir_all(folder_to_path(&folder, target))?;
        folders.push(folder);
    }
    // Create second-level folders.
    let mut sub_candidates: Vec<&str> = SUBFOLDER_CANDIDATES_ORIG.to_vec();
    sub_candidates.shuffle(rng);
    for &parent in TOP_LEVEL_FOLDERS.iter() {
        // Randomly choose a number of subfolders using an inclusive range.
        let count = rng.random_range(min_subfolders..=max_subfolders);
        let chosen: Vec<&str> = sub_candidates.iter().take(count).copied().collect();
        sub_candidates.drain(0..count);
        sub_candidates.extend(chosen.iter().copied());
        for sub in chosen {
            let folder = Folder::Second {
                parent: parent.to_string(),
                name: sub.to_string(),
            };
            fs::create_dir_all(folder_to_path(&folder, target))?;
            folders.push(folder);
        }
    }
    Ok(folders)
}

/// Choose a folder (as a Folder enum) from a slice of folders using fixed placement weights.
/// Groups: Root, First, Second.
fn pick_folder(folders: &[Folder], rng: &mut ChaCha8Rng) -> Folder {
    // Partition folders by group.
    let mut roots = Vec::new();
    let mut firsts = Vec::new();
    let mut seconds = Vec::new();
    for folder in folders {
        match folder {
            Folder::Root => roots.push(folder),
            Folder::First(_) => firsts.push(folder),
            Folder::Second { .. } => seconds.push(folder),
        }
    }

    let groups = [roots, firsts, seconds];
    let total_weight: f64 = PLACEMENT_WEIGHTS.iter().sum();
    let r = rng.random_range(0.0..total_weight);
    let mut acc = 0.0;
    for (item, weight) in groups.iter().zip(PLACEMENT_WEIGHTS.iter()) {
        acc += weight;
        if r <= acc {
            return (*item.choose(rng).unwrap()).clone();
        }
    }

    folders[0].clone()
}

/// Generate a three‑word filename with a random file extension.
fn generate_three_word_filename(rng: &mut ChaCha8Rng) -> String {
    let words = FILENAME_WORDS.choose_multiple(rng, 3);
    let extension = FILE_EXTENSIONS.choose(rng).unwrap();
    format!(
        "{}{}",
        words.into_iter().cloned().collect::<Vec<&str>>().join("-"),
        extension
    )
}

/// Write a file at the given path with the specified size.
/// This version uses a fixed 1KB block generated from alphanumeric characters.
fn write_random_file(filepath: &Path, size_bytes: u64, rng: &mut ChaCha8Rng) -> io::Result<()> {
    let size_bytes = size_bytes as usize;
    if let Some(parent) = filepath.parent() {
        fs::create_dir_all(parent)?;
    }

    const BLOCK_SIZE: usize = 1024;
    // Generate one block of truly random data.
    let mut block = vec![0u8; BLOCK_SIZE];
    rng.fill_bytes(&mut block);

    let file = File::create(filepath)?;
    let mut writer = BufWriter::with_capacity(128 * 1024, file);

    let full_blocks = size_bytes / BLOCK_SIZE;
    let remainder = size_bytes % BLOCK_SIZE;

    // Write the full blocks in a loop.
    for _ in 0..full_blocks {
        writer.write_all(&block)?;
    }
    // Write any remaining bytes.
    if remainder > 0 {
        writer.write_all(&block[..remainder])?;
    }

    // Flush once at the end to reduce system calls.
    writer.flush()?;
    Ok(())
}

/// Collect file creation tasks until the total output bytes exceed the target.
/// Tasks are stored in a HashMap keyed by the final file path (to avoid duplicates).
fn collect_tasks(
    total_target: u64,
    sizes: &[FileSizeCandidate],
    weights: &[f64],
    rng: &mut ChaCha8Rng,
    folders: &[Folder],
) -> (HashMap<PathBuf, FileTask>, u64) {
    let mut tasks: HashMap<PathBuf, FileTask> = HashMap::new();
    let mut accumulated: u64 = 0;
    let total_weight: f64 = weights.iter().sum();

    while accumulated < total_target {
        // Weighted selection: choose candidate index.
        let r = rng.random_range(0.0..total_weight);
        let mut acc = 0.0;
        let mut chosen_idx = 0;
        for (i, w) in weights.iter().enumerate() {
            acc += w;
            if r <= acc {
                chosen_idx = i;
                break;
            }
        }
        let candidate = sizes[chosen_idx];

        // Choose a folder and generate a filename.
        let folder = pick_folder(folders, rng);
        let path = folder_to_path(&folder, &PathBuf::from("."));
        let filename = generate_three_word_filename(rng);
        let filepath = path.join(filename);

        // Only add if this filepath is not already used.
        if !tasks.contains_key(&filepath) {
            tasks.insert(
                filepath.clone(),
                FileTask {
                    size_bytes: candidate.bytes,
                    filepath,
                },
            );
            accumulated += candidate.bytes;
        }
    }
    (tasks, accumulated)
}

const KB: u64 = 1024;
const MB: u64 = KB * 1024;
const GB: u64 = MB * 1024;

pub(crate) fn human_readable_size(bytes: u64) -> String {
    let (value, unit) = match bytes {
        GB.. => (bytes as f64 / GB as f64, "GB"),
        MB.. => (bytes as f64 / MB as f64, "MB"),
        KB.. => (bytes as f64 / KB as f64, "KB"),
        0.. => (bytes as f64, "B"),
    };

    let value = if value < 10.0 {
        format!("{:.1}", value)
    } else {
        format!("{:.0}", value)
    };

    format!("{}{}", value, unit)
}

/// Display statistics computed from the file tasks using comfy-table.
fn display_stats(
    sizes: &[FileSizeCandidate],
    tasks: &HashMap<PathBuf, FileTask>,
    total_accumulated: u64,
) {
    // Compute counts and total bytes per candidate file size.
    let mut stats: HashMap<u64, (u64, u64)> = HashMap::new();
    for task in tasks.values() {
        let entry = stats.entry(task.size_bytes).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += task.size_bytes;
    }
    let mut table = Table::new();
    table
        .set_header(vec!["File size", "Files", "Total Bytes", "% of Total"])
        .set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
    for candidate in sizes {
        let (count, total_bytes) = stats.get(&candidate.bytes).cloned().unwrap_or((0, 0));
        let pct = if total_accumulated > 0 {
            (total_bytes as f64) / (total_accumulated as f64) * 100.0
        } else {
            0.0
        };
        table.add_row(vec![
            Cell::new(human_readable_size(candidate.bytes)).set_alignment(CellAlignment::Right),
            Cell::new(count).set_alignment(CellAlignment::Right),
            Cell::new(human_readable_size(total_bytes)).set_alignment(CellAlignment::Right),
            Cell::new(format!("{:.2}%", pct)).set_alignment(CellAlignment::Right),
        ]);
    }
    println!("\nSummary per file size band:");
    println!("{table}");
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();

    // Parse total target size.
    let total_target = parse_size(&cli.total);

    // Parse candidate file sizes.
    let sizes: Vec<FileSizeCandidate> = cli
        .sizes
        .split(',')
        .map(|b| FileSizeCandidate {
            bytes: parse_size(b),
        })
        .collect();
    if sizes.is_empty() {
        eprintln!("No valid file sizes provided.");
        std::process::exit(1);
    }

    // Compute weights: weight = (file_size)^(–alpha)
    let weights: Vec<f64> = sizes
        .iter()
        .map(|s| (s.bytes as f64).powf(-cli.alpha))
        .collect();

    // Prepare target directory.
    fs::create_dir_all(&cli.target_path)?;
    std::env::set_current_dir(&cli.target_path)?;

    println!("Generating deterministic random files...");

    // Set up RNG.
    let mut rng = ChaCha8Rng::seed_from_u64(cli.seed);

    // Create folder structure (as structured Folder values).
    let folders =
        create_folders_and_return_folders(&cli.target_path, &mut rng, cli.min_dirs, cli.max_dirs)?;

    // Collect file creation tasks.
    let (tasks, accumulated) = collect_tasks(total_target, &sizes, &weights, &mut rng, &folders);
    let total_tasks = tasks.len();
    println!(
        "Placing {} files with a total of {} bytes (target was {})...",
        total_tasks,
        human_readable_size(accumulated),
        human_readable_size(accumulated)
    );
    display_stats(&sizes, &tasks, accumulated);

    // Convert tasks into a Vec.
    let tasks: Vec<FileTask> = tasks.into_values().collect();

    // Create progress bar.
    let progress = ProgressBar::new(total_tasks as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta} remaining)")
            .expect("Failed to set progress bar style")
            .progress_chars("##-"),
    );
    let progress = Mutex::new(progress);

    // Process file creation tasks in parallel.
    tasks.par_iter().for_each(|task| {
        // Create a task-specific RNG using a hash of the file path and the CLI seed.
        let mut hasher = DefaultHasher::new();
        task.filepath.hash(&mut hasher);
        let file_seed = cli.seed.wrapping_add(hasher.finish());
        let mut task_rng = ChaCha8Rng::seed_from_u64(file_seed);
        if !task.filepath.exists() {
            if let Err(e) = write_random_file(&task.filepath, task.size_bytes, &mut task_rng) {
                eprintln!("Failed to write {:?}: {}", task.filepath, e);
            }
        }
        let pb = progress.lock().unwrap();
        pb.inc(1);
    });
    progress.into_inner().unwrap().finish_with_message("Done!");
    println!("Folder structure and files created.");

    Ok(())
}

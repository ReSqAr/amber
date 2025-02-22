use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::{IndexedRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;
use std::{
    collections::hash_map::{DefaultHasher, HashMap},
    fs::{self, File},
    hash::{Hash, Hasher},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

/// Represents a file generation band with a specified file size and count.
struct FileBand {
    size_bytes: usize,
    count: usize,
}

// Define the file bands to be generated.
const FILE_BANDS: &[FileBand] = &[
    FileBand { size_bytes: 1 * 1024, count: 30000 },         // 1KB files
    FileBand { size_bytes: 100 * 1024, count: 20000 },       // 100KB files
    FileBand { size_bytes: 10 * 1024 * 1024, count: 800 },   // 10MB files
];

const TOP_LEVEL_FOLDERS: &[&str] = &[
    "Behemoth", "Canterbury", "Eros", "Pella", "Rocinante", "Razorback", "Tycho",
];

const SUBFOLDER_CANDIDATES_ORIG: &[&str] = &[
    "Aberrant", "AbernathyStation", "AgathaKing", "AndersonStation", "Arboghast",
    "Artemis", "Basalt", "Belter", "Ceres", "Chronicle", "Cipher", "Ganymede",
    "Kaguya", "Kyiv", "Laconia", "MaoKwikowski", "Martian", "Nauvoo", "Persephone",
    "Phoebe", "Plasma", "Protogen", "Somnambulist", "Stardust",
];

const FILENAME_WORDS: &[&str] = &[
    "amber", "asteroid", "basalt", "belter", "burn", "cascade", "cipher", "chronicle",
    "cinder", "cobalt", "cosmos", "cradle", "delta", "drift", "drone", "ejecta", "eon",
    "expanse", "fusion", "gate", "horizon", "injection", "kinetic", "knot", "laser", "lunar",
    "metal", "nova", "orbital", "plasma", "proto", "quantum", "raze", "ring", "spin", "station",
    "system", "thrust", "torus", "vacuum", "void", "wander", "whisper", "volt", "xenon", "zenith",
];

const FILE_EXTENSIONS: &[&str] = &[".txt", ".md", ".log", ".dat", ".cfg", ".bin"];

/// Placement weights for files based on folder depth: [root, top-level, second-level].
const PLACEMENT_WEIGHTS: &[f64] = &[0.001, 0.05, 0.95];

/// Represents a task to create a file with a given size and target path.
struct FileTask {
    size_bytes: usize,
    filepath: PathBuf,
}

/// Command-line arguments.
#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    /// Target directory where files and folders will be created.
    target_path: PathBuf,

    /// Optional random seed (default is 42)
    #[arg(short, long, default_value_t = 42)]
    seed: u64,
}

/// Create a two-level folder structure inside the given target directory.
/// Returns a vector with all folder paths (including the target itself).
fn create_folders_and_return_paths(target: &Path, rng: &mut ChaCha8Rng) -> io::Result<Vec<PathBuf>> {
    let mut all_paths = vec![target.to_path_buf()];
    let mut subfolder_candidates: Vec<&str> = SUBFOLDER_CANDIDATES_ORIG.to_vec();
    subfolder_candidates.shuffle(rng);

    for &folder in TOP_LEVEL_FOLDERS.iter() {
        let top_folder = target.join(folder);
        fs::create_dir_all(&top_folder)?;
        all_paths.push(top_folder.clone());

        // Pick a random number of subfolders between 2 and 5.
        let count_subfolders = rng.random_range(2..=5);
        let chosen: Vec<&str> = subfolder_candidates.iter().take(count_subfolders).copied().collect();

        // Rotate the chosen candidates to the back.
        subfolder_candidates.drain(0..count_subfolders);
        subfolder_candidates.extend(chosen.iter().copied());

        for sub in chosen {
            let sub_path = top_folder.join(sub);
            fs::create_dir_all(&sub_path)?;
            all_paths.push(sub_path);
        }
    }
    Ok(all_paths)
}

/// Choose a folder from the list based on placement weights.
fn pick_folder_path(all_paths: &[PathBuf], rng: &mut ChaCha8Rng) -> PathBuf {
    let mut root_paths = vec![];
    let mut top_level_paths = vec![];
    let mut second_level_paths = vec![];

    for p in all_paths {
        // Depth is calculated relative to the target: depth 0 for the target itself.
        let depth = p.components().count() - 1;
        if depth == 0 {
            root_paths.push(p.clone());
        } else if depth == 1 {
            top_level_paths.push(p.clone());
        } else {
            second_level_paths.push(p.clone());
        }
    }

    let groups = ["root", "top", "second"];
    let group_choice = groups
        .choose_weighted(rng, |&g| match g {
            "root" => PLACEMENT_WEIGHTS[0],
            "top" => PLACEMENT_WEIGHTS[1],
            "second" => PLACEMENT_WEIGHTS[2],
            _ => 0.0,
        })
        .unwrap();

    match *group_choice {
        "root" => root_paths[0].clone(),
        "top" => top_level_paths.choose(rng).unwrap_or(&root_paths[0]).clone(),
        _ => second_level_paths.choose(rng).unwrap_or(&root_paths[0]).clone(),
    }
}

/// Generate a three-word filename with a random file extension.
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
/// A 1KB block of deterministic random bytes is generated and repeated.
fn write_random_file(filepath: &Path, size_bytes: usize, rng: &mut ChaCha8Rng) -> io::Result<()> {
    if let Some(parent) = filepath.parent() {
        fs::create_dir_all(parent)?;
    }
    let block_size = 1024;
    let pool: Vec<u8> = (b'A'..=b'Z')
        .chain(b'a'..=b'z')
        .chain(b'0'..=b'9')
        .collect();
    let block: Vec<u8> = (0..block_size)
        .map(|_| *pool.choose(rng).unwrap())
        .collect();

    let mut file = File::create(filepath)?;
    let full_blocks = size_bytes / block_size;
    let remainder = size_bytes % block_size;

    for _ in 0..full_blocks {
        file.write_all(&block)?;
    }
    if remainder > 0 {
        file.write_all(&block[..remainder])?;
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();

    // Ensure the target directory exists and switch into it.
    fs::create_dir_all(&cli.target_path)?;
    std::env::set_current_dir(&cli.target_path)?;

    println!("Generating deterministic random files...");

    // Initialize the RNG with the provided seed.
    let mut rng = ChaCha8Rng::seed_from_u64(cli.seed);
    let all_paths = create_folders_and_return_paths(Path::new("."), &mut rng)?;

    // Precompute all file creation tasks using a HashMap to avoid duplicate paths.
    let mut tasks: HashMap<PathBuf, FileTask> = HashMap::new();
    for band in FILE_BANDS.iter() {
        for _ in 0..band.count {
            let folder = pick_folder_path(&all_paths, &mut rng);
            let filename = generate_three_word_filename(&mut rng);
            let filepath = folder.join(filename);
            // Only insert if the filepath is not already used.
            tasks.entry(filepath.clone()).or_insert(FileTask {
                size_bytes: band.size_bytes,
                filepath,
            });
        }
    }
    let total_tasks = tasks.len();
    println!("Placing {} files (after deduplication)...", total_tasks);

    // Create a progress bar using indicatif.
    let progress = ProgressBar::new(total_tasks as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta} remaining)")
            .expect("Failed to set progress bar style")
            .progress_chars("##-"),
    );

    // Global seed used for task-specific RNG initialization.
    let global_seed = cli.seed;
    // Wrap the progress bar in a Mutex to update it safely.
    let progress = Mutex::new(progress);

    // Process file creation tasks in parallel.
    tasks
        .values()
        .par_bridge()
        .for_each(|task| {
            // Create a task-specific RNG by hashing the filepath with the global seed.
            let mut hasher = DefaultHasher::new();
            task.filepath.hash(&mut hasher);
            let file_seed = global_seed.wrapping_add(hasher.finish());
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

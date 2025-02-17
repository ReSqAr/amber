const KB: u64 = 1024;
const MB: u64 = KB * 1024;
const GB: u64 = MB * 1024;
const TB: u64 = GB * 1024;
const PB: u64 = TB * 1024;
const EB: u64 = PB * 1024;

const B_MAX: u64 = KB - 1;
const KB_MAX: u64 = MB - 1;
const MB_MAX: u64 = GB - 1;
const GB_MAX: u64 = TB - 1;
const TB_MAX: u64 = PB - 1;
const PB_MAX: u64 = EB - 1;

pub(crate) fn human_readable_size(bytes: u64) -> String {
    match bytes {
        0..=B_MAX => format!("{}B", bytes),
        KB..=KB_MAX => format!("{:.2}KB", bytes as f64 / KB as f64),
        MB..=MB_MAX => format!("{:.2}MB", bytes as f64 / MB as f64),
        GB..=GB_MAX => format!("{:.2}GB", bytes as f64 / GB as f64),
        TB..=TB_MAX => format!("{:.2}TB", bytes as f64 / TB as f64),
        PB..=PB_MAX => format!("{:.2}PB", bytes as f64 / PB as f64),
        _ => format!("{:.2} EB", bytes as f64 / EB as f64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_human_readable_size() {
        assert_eq!(human_readable_size(500), "500B");
        assert_eq!(human_readable_size(1500), "1.46KB");
        assert_eq!(human_readable_size(1024 * 1024), "1.00MB");
        assert_eq!(human_readable_size(1024 * 1024 * 1024), "1.00GB");
    }
}

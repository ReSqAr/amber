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
    let (value, unit) = match bytes {
        0..=B_MAX => (bytes as f64, "B"),
        KB..=KB_MAX => (bytes as f64 / KB as f64, "KB"),
        MB..=MB_MAX => (bytes as f64 / MB as f64, "MB"),
        GB..=GB_MAX => (bytes as f64 / GB as f64, "GB"),
        TB..=TB_MAX => (bytes as f64 / TB as f64, "TB"),
        PB..=PB_MAX => (bytes as f64 / PB as f64, "PB"),
        _ => (bytes as f64 / EB as f64, "EB"),
    };

    let value = if value < 10.0 {
        format!("{:.1}", value)
    } else {
        format!("{:.0}", value)
    };

    format!("{}{}", value, unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_human_readable_size() {
        assert_eq!(human_readable_size(500), "500B");
        assert_eq!(human_readable_size(1500), "1.5KB");
        assert_eq!(human_readable_size(150 * 1024 + 1), "150KB");
        assert_eq!(human_readable_size(1024 * 1024), "1.0MB");
        assert_eq!(human_readable_size(150 * 1024 * 1024 + 1), "150MB");
        assert_eq!(human_readable_size(1024 * 1024 * 1024), "1.0GB");
        assert_eq!(human_readable_size(1024 * 1024 * 1024 * 1024), "1.0TB");
        assert_eq!(
            human_readable_size(1024 * 1024 * 1024 * 1024 * 1024),
            "1.0PB"
        );
        assert_eq!(
            human_readable_size(1024 * 1024 * 1024 * 1024 * 1024 * 1024),
            "1.0EB"
        );
    }
}

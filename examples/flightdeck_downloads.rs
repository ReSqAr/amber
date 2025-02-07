use amber::flightdeck;
use amber::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObservable, BaseObservation, StateTransformer, TerminationAction,
};
use amber::flightdeck::observer::Observer;
use amber::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::time::sleep;

fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|x| x.as_millis() as i64)
        .unwrap_or(0)
}

#[tokio::main]
async fn main() {
    let log_path: PathBuf = format!("debug.{}.txt", current_timestamp()).into();
    flightdeck::flightdeck(
        async {
            wrapped().await;
            Ok::<(), Box<dyn std::error::Error>>(())
        },
        root_builders(),
        log_path,
        None,
        None,
    )
    .await
    .expect("Failed to run flightdeck");
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let file = BaseLayoutBuilderBuilder::default()
        .type_key("file")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdStateFn(Box::new(|_, id, _| {
            id.unwrap_or("<unknown>".into())
        })))
        .infallible_build()
        .boxed();

    let overall = BaseLayoutBuilderBuilder::default()
        .type_key("overall")
        .state_transformer(StateTransformer::Static {
            msg: "downloading".to_string(),
            done: "downloaded".to_string(),
        })
        .infallible_build()
        .boxed();

    [LayoutItemBuilderNode::from(overall).add_child(file)]
}

async fn wrapped() {
    let total_downloads = 10_000;
    let finished_counter = Arc::new(AtomicU64::new(0));

    {
        let overall_counter = finished_counter.clone();
        tokio::spawn(async move {
            let mut overall_obs = Observer::<BaseObservable>::without_id("overall");
            overall_obs
                .observe(log::Level::Trace, BaseObservation::Length(total_downloads))
                .observe(
                    log::Level::Trace,
                    BaseObservation::State("downloading".into()),
                );

            loop {
                let finished = overall_counter.load(Ordering::SeqCst);
                overall_obs.observe(log::Level::Info, BaseObservation::Position(finished));
                if finished >= total_downloads {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
        });
    }

    use rand_distr::{Distribution, LogNormal};
    let log_normal = LogNormal::new(1.0, 3.0).unwrap();

    for i in 0..total_downloads {
        let finished_counter = finished_counter.clone();
        let millis = if i == 10 {
            100
        } else {
            log_normal.sample(&mut rand::rng()) as u64
        };

        tokio::spawn(async move {
            // initial delay
            sleep(Duration::from_millis(i * 10)).await;

            let id = format!("file #{}", i);
            let mut download_obs = Observer::with_auto_termination(
                BaseObservable::with_id("file", id),
                log::Level::Warn,
                BaseObservation::TerminalState("done (dropped)".into()),
            );
            download_obs
                .observe(log::Level::Trace, BaseObservation::Length(100))
                .observe(
                    log::Level::Debug,
                    BaseObservation::State("in progress".into()),
                );

            // simulate the download progress from 0 to 98 - when dropped jumps to 100
            for progress in 0..=98 {
                download_obs.observe(log::Level::Trace, BaseObservation::Position(progress));

                if i == 10 && progress == 47 {
                    let x: std::result::Result<(), anyhow::Error> =
                        Err(anyhow::anyhow!("this is an error"));
                    let _ = x.map_err(
                        download_obs
                            .observe_error(|_| BaseObservation::TerminalState("error".into())),
                    );
                    return;
                }

                sleep(Duration::from_millis(millis)).await;
            }

            if i % 2 == 0 {
                download_obs.observe(
                    log::Level::Info,
                    BaseObservation::TerminalState("done (explicit)".into()),
                );
            }

            finished_counter.fetch_add(1, Ordering::SeqCst);
        });
    }

    loop {
        if finished_counter.load(Ordering::SeqCst) >= total_downloads {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    println!("All downloads completed.");
}

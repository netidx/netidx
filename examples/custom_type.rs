const VAR_CONTAINER: &str = "START_NETIDX_CONTAINER";

fn run_container() -> Result<()> {
    if let Some(_) = env::var(VAR_CONTAINER).ok() {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(async { netidx_contai })
    }
    Ok(())
}

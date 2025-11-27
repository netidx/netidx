//! Container Server Example - Employee Directory
//!
//! This example starts an embedded netidx-container with an employee directory demonstrating:
//! - Tables (named rows/columns for employee records)
//! - Sheets (numeric-indexed grid for timesheets)
//! - Initial data setup
//!
//! ## Running
//!
//! Start this first in one terminal:
//! ```bash
//! cargo run --example container_server
//! ```
//!
//! Then in another terminal, run the client to perform CRUD operations:
//! ```bash
//! cargo run --example container_client
//! ```
//!
//! Press Ctrl+C to stop the server.

#[macro_use]
extern crate netidx_protocols;

use anyhow::Result;
use arcstr::ArcStr;
use futures::channel::mpsc;
use netidx::{config::Config, path::Path, publisher::Publisher};
use netidx_container::{Container, ParamsBuilder, Txn};
use netidx_protocols::rpc::server::{ArgSpec, Proc, RpcCall};
use netidx_value::Value;
use tempdir::TempDir;
use tokio::sync::oneshot;

/// Employee add/change record
struct Employee {
    id: ArcStr,
    name: Option<ArcStr>,
    email: Option<ArcStr>,
    department: Option<ArcStr>,
    salary: Option<u64>,
    phone: Option<ArcStr>,
}

impl Employee {
    fn add_to_txn(&self, txn: &mut Txn, base: &Path) {
        let emp_path = base.join(&*self.id);
        if let Some(name) = &self.name {
            txn.set_data(true, emp_path.join("name"), Value::from(name.clone()), None);
        }
        if let Some(email) = &self.email {
            txn.set_data(true, emp_path.join("email"), Value::from(email.clone()), None);
        }
        if let Some(department) = &self.department {
            txn.set_data(
                true,
                emp_path.join("department"),
                Value::from(department.clone()),
                None,
            );
        }
        if let Some(salary) = &self.salary {
            txn.set_data(true, emp_path.join("salary"), Value::V64(salary.clone()), None);
        }
        if let Some(phone) = &self.phone {
            txn.set_data(true, emp_path.join("phone"), Value::from(phone.clone()), None);
        }
    }
}

/// Start custom RPC for adding employees atomically
fn start_set_employee_rpc(
    publisher: &Publisher,
    base: &Path,
    container: Container,
) -> Result<Proc> {
    let dir = base.join("directory");

    let handler = move |mut call: RpcCall,
                        id: ArcStr,
                        name: Option<ArcStr>,
                        email: Option<ArcStr>,
                        department: Option<ArcStr>,
                        salary: Option<u64>,
                        phone: Option<ArcStr>|
          -> Option<()> {
        let container = container.clone();
        let dir = dir.clone();

        tokio::spawn(async move {
            let mut txn = Txn::new();
            Employee { id, name, email, department, salary, phone }
                .add_to_txn(&mut txn, &dir);
            match container.commit(txn).await {
                Ok(()) => call.reply.send(Value::Null),
                Err(e) => call.reply.send(Value::error(format!("{}", e))),
            }
        });

        None
    };

    define_rpc!(
        publisher,
        base.append("rpcs/set-employee"),
        "Add or update an employee atomically",
        handler,
        None::<mpsc::Sender<()>>,
        id: ArcStr = Value::Null; "employee ID (required)",
        name: Option<ArcStr> = Value::Null; "employee name (optional)",
        email: Option<ArcStr> = Value::Null; "employee email (optional)",
        department: Option<ArcStr> = Value::Null; "employee department (optional)",
        salary: Option<u64> = Value::Null; "employee salary (optional)",
        phone: Option<ArcStr> = Value::Null; "employee phone number (optional)"
    )
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    println!("╔════════════════════════════════════════════════╗");
    println!("║  Container Server - Employee Directory         ║");
    println!("╚════════════════════════════════════════════════╝\n");

    // Create temporary database directory
    let temp = TempDir::new("netidx_container_example")?;
    let db_path = format!("{}", temp.path().display());
    println!("Database: {}\n", db_path);

    // Define base path
    let base = Path::from("/local/employees");

    // Start container
    let auth = cfg.default_auth();
    let params = ParamsBuilder::default().db(db_path).api_path(base.clone()).build()?;
    let container = Container::start(cfg, auth, params).await?;

    println!("=== Initializing Employee Directory ===\n");

    // Create root and initial data
    let mut txn = Txn::new();
    let dir = base.join("directory");
    txn.add_root(dir.clone(), None);
    txn.set_locked(dir.clone(), None);

    // Add initial employee table data
    let employees = [
        Employee {
            id: "emp001".into(),
            name: Some("Alice Smith".into()),
            email: Some("alice@company.com".into()),
            department: Some("Engineering".into()),
            salary: Some(75000),
            phone: None,
        },
        Employee {
            id: "emp002".into(),
            name: Some("Bob Johnson".into()),
            email: Some("bob@company.com".into()),
            department: Some("Sales".into()),
            salary: Some(65000),
            phone: None,
        },
        Employee {
            id: "emp003".into(),
            name: Some("Carol Williams".into()),
            email: Some("carol@company.com".into()),
            department: Some("Marketing".into()),
            salary: Some(70000),
            phone: None,
        },
    ];

    for employee in &employees {
        employee.add_to_txn(&mut txn, &dir);
    }

    container.commit(txn).await?;

    println!("✓ Created employee directory table:");
    println!("  - 3 employees (emp001, emp002, emp003)");
    println!("  - 4 columns (name, email, department, salary)");

    // Start custom add_employee RPC
    let publisher = container.publisher().await?;
    let _set_employee_rpc = start_set_employee_rpc(&publisher, &base, container.clone())?;

    println!("\n=== Container Ready ===");
    println!("RPC endpoints available at: {}/rpcs", base);
    println!("  - Built-in: set-data, delete, delete-subtree, etc.");
    println!("  - Custom: set-employee (atomic employee creation and update)");
    println!("Data published at: {}", base);
    println!("\nRun the client in another terminal:");
    println!("  cargo run --example container_client\n");
    println!("Press Ctrl+C to stop the server\n");

    // Wait for Ctrl+C
    let (tx_stop, rx_stop) = oneshot::channel();
    let mut tx_stop = Some(tx_stop);
    ctrlc::set_handler(move || {
        if let Some(tx) = tx_stop.take() {
            let _ = tx.send(());
        }
    })?;

    let _ = rx_stop.await;

    println!("\n\nShutting down...");
    drop(container);
    drop(temp); // Clean up temp directory

    Ok(())
}

fn main() -> Result<()> {
    env_logger::init();
    Config::maybe_run_machine_local_resolver()?;
    tokio_main(Config::load_default_or_local_only()?)
}

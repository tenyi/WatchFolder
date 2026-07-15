//! 目錄監控程式：監聽指定資料夾的檔案變動，比對 SHA256 雜湊值後透過 SMTP 發送電子郵件通知。
//!
//! 使用方式：
//! 1. 複製 `.env.example` 為 `.env` 並填入 SMTP 設定
//! 2. `cargo run -- <監控目錄> <通知電子郵件>`

use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::Local;
use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use notify::RecursiveMode;
use notify_debouncer_full::{new_debouncer, DebounceEventResult, DebouncedEvent};
use sha2::{Digest, Sha256};

/// 雜湊紀錄檔名稱，與監控目錄放在同一層
const HASHES_FILENAME: &str = "hashes.txt";

/// 預設 debounce 等待時間（毫秒），避免檔案寫入中途觸發雜湊計算
const DEFAULT_DEBOUNCE_MS: u64 = 500;

fn main() {
    if let Err(e) = run() {
        eprintln!("錯誤: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    // 載入專案根目錄的 .env（檔案不存在時不視為錯誤）
    dotenvy::dotenv().ok();

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        bail!("使用方式: {} <監控目錄> <通知電子郵件>", args[0]);
    }

    let watch_dir = PathBuf::from(&args[1]);
    let notify_email = args[2].clone();

    if !watch_dir.is_dir() {
        bail!("監控目錄不存在或不是資料夾: {}", watch_dir.display());
    }

    let smtp = SmtpConfig::from_env()?;
    let debounce_ms = env::var("DEBOUNCE_MS")
        .ok()
        .map(|v| v.parse::<u64>())
        .transpose()
        .context("DEBOUNCE_MS 必須為正整數")?
        .unwrap_or(DEFAULT_DEBOUNCE_MS);

    let hashes_path = watch_dir.join(HASHES_FILENAME);
    let mut file_hashes = load_hashes(&watch_dir, &hashes_path)?;
    write_hashes(&file_hashes, &hashes_path)?;

    // 單一背景 worker 依序寄信，避免大量 thread::spawn
    let email_tx = spawn_email_worker(smtp);

    let (debounce_tx, debounce_rx) = mpsc::channel::<DebounceEventResult>();
    let mut debouncer = new_debouncer(
        Duration::from_millis(debounce_ms),
        None,
        move |result| {
            if debounce_tx.send(result).is_err() {
                eprintln!("debouncer channel 已關閉");
            }
        },
    )
    .context("無法建立檔案監聽 debouncer")?;

    debouncer
        .watch(&watch_dir, RecursiveMode::Recursive)
        .with_context(|| format!("無法監聽目錄: {}", watch_dir.display()))?;

    println!(
        "開始監控 {}，通知信箱: {}",
        watch_dir.display(),
        notify_email
    );

    for result in debounce_rx {
        match result {
            Ok(events) => {
                for event in events {
                    // 單一事件處理失敗只記錄並繼續，不讓暫時性錯誤（讀檔/寄信/寫盤）終止整個監控
                    if let Err(e) = process_debounced_event(
                        &event,
                        &watch_dir,
                        &hashes_path,
                        &mut file_hashes,
                        &notify_email,
                        &email_tx,
                    ) {
                        eprintln!("處理事件失敗，已略過: {e:#}");
                    }
                }
            }
            Err(errors) => {
                for e in errors {
                    eprintln!("監視錯誤: {e:?}");
                }
            }
        }
    }

    Ok(())
}

/// SMTP 連線設定，從環境變數載入
struct SmtpConfig {
    host: String,
    port: u16,
    username: String,
    password: String,
    from: String,
}

impl SmtpConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            host: require_env("SMTP_HOST")?,
            port: env::var("SMTP_PORT")
                .unwrap_or_else(|_| "587".to_string())
                .parse()
                .context("SMTP_PORT 必須為有效埠號")?,
            username: require_env("SMTP_USERNAME")?,
            password: require_env("SMTP_PASSWORD")?,
            from: require_env("SMTP_FROM")?,
        })
    }
}

fn require_env(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("缺少必要的環境變數: {key}"))
}

/// 待寄出的郵件工作項目
struct EmailJob {
    to: String,
    body: String,
}

/// 建立單一背景執行緒處理所有寄信任務
fn spawn_email_worker(smtp: SmtpConfig) -> Sender<EmailJob> {
    let (tx, rx) = mpsc::channel::<EmailJob>();

    thread::spawn(move || {
        let mailer = match build_mailer(&smtp) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("無法建立 SMTP 連線: {e:#}");
                return;
            }
        };

        for job in rx {
            if let Err(e) = send_email(&mailer, &smtp.from, &job.to, &job.body) {
                eprintln!("無法發送電子郵件至 {}: {e:#}", job.to);
            } else {
                println!("電子郵件已成功發送至 {}", job.to);
            }
        }
    });

    tx
}

fn build_mailer(smtp: &SmtpConfig) -> Result<SmtpTransport> {
    let creds = Credentials::new(smtp.username.clone(), smtp.password.clone());
    Ok(SmtpTransport::relay(&smtp.host)
        .with_context(|| format!("無法連線至 SMTP 伺服器: {}", smtp.host))?
        .port(smtp.port)
        .credentials(creds)
        .build())
}

fn send_email(
    mailer: &SmtpTransport,
    from: &str,
    to: &str,
    body: &str,
) -> Result<()> {
    let email = Message::builder()
        .from(from.parse::<Mailbox>().context("SMTP_FROM 格式無效")?)
        .to(to.parse::<Mailbox>().context("通知電子郵件格式無效")?)
        .subject("檔案變動通知")
        .body(body.to_string())?;

    mailer
        .send(&email)
        .context("SMTP 寄信失敗")?;
    Ok(())
}

fn process_debounced_event(
    event: &DebouncedEvent,
    watch_dir: &Path,
    hashes_path: &Path,
    file_hashes: &mut HashMap<String, String>,
    notify_email: &str,
    email_tx: &Sender<EmailJob>,
) -> Result<()> {
    // 若系統回報可能漏事件，重新掃描整個目錄並比對差異
    if event.need_rescan() {
        eprintln!("收到 rescan 旗標，重新掃描目錄...");
        rescan_and_notify(watch_dir, hashes_path, file_hashes, notify_email, email_tx)?;
        return Ok(());
    }

    // 若事件僅涉及 hashes.txt 本身，略過避免無限迴圈
    if event
        .paths
        .iter()
        .all(|p| is_hashes_file(p, hashes_path))
    {
        return Ok(());
    }

    use notify::EventKind;

    match &event.kind {
        EventKind::Create(_) | EventKind::Modify(_) => {
            let mut changed = false;
            for path in &event.paths {
                if path.is_file() && !is_hashes_file(path, hashes_path) {
                    if handle_file_change(file_hashes, path, notify_email, email_tx)? {
                        changed = true;
                    }
                }
            }
            if changed {
                write_hashes(file_hashes, hashes_path)?;
            }
        }
        EventKind::Remove(_) => {
            let mut changed = false;
            for path in &event.paths {
                if is_hashes_file(path, hashes_path) {
                    continue;
                }
                if handle_file_remove(file_hashes, path, notify_email, email_tx)? {
                    changed = true;
                }
            }
            if changed {
                write_hashes(file_hashes, hashes_path)?;
            }
        }
        _ => {}
    }

    Ok(())
}

/// 重新掃描目錄，比對雜湊差異並發送通知
fn rescan_and_notify(
    watch_dir: &Path,
    hashes_path: &Path,
    file_hashes: &mut HashMap<String, String>,
    notify_email: &str,
    email_tx: &Sender<EmailJob>,
) -> Result<()> {
    // 先以新掃描結果取代，確保即使後續通知失敗，雜湊表仍為最新基準（不會變空）
    let old_hashes = std::mem::replace(file_hashes, scan_directory(watch_dir, hashes_path)?);
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");

    // 新增或修改的檔案
    for (path, new_hash) in file_hashes.iter() {
        match old_hashes.get(path) {
            Some(old_hash) if old_hash == new_hash => {}
            Some(old_hash) => {
                let body = format!(
                    "檔案變動時間: {timestamp}\n路徑: {path}\n原始雜湊值: {old_hash}\n新的雜湊值: {new_hash}"
                );
                queue_email(email_tx, notify_email, body)?;
            }
            None => {
                let body = format!(
                    "檔案變動時間: {timestamp}\n路徑: {path}\n新的雜湊值: {new_hash}"
                );
                queue_email(email_tx, notify_email, body)?;
            }
        }
    }

    // 已刪除的檔案
    for (path, old_hash) in &old_hashes {
        if !file_hashes.contains_key(path) {
            let body = format!(
                "檔案刪除時間: {timestamp}\n路徑: {path}\n刪除前雜湊值: {old_hash}"
            );
            queue_email(email_tx, notify_email, body)?;
        }
    }

    write_hashes(file_hashes, hashes_path)?;
    Ok(())
}

fn is_hashes_file(path: &Path, hashes_path: &Path) -> bool {
    path == hashes_path
        || path
            .file_name()
            .is_some_and(|name| name == Path::new(HASHES_FILENAME))
}

/// 將路徑轉為一致的 key（優先使用 canonical 絕對路徑）
fn path_key(path: &Path) -> Result<String> {
    let resolved = path
        .canonicalize()
        .unwrap_or_else(|_| path.to_path_buf());
    Ok(resolved.to_string_lossy().into_owned())
}

/// 載入 `hashes.txt`；若不存在則遞迴掃描監控目錄建立基準雜湊
fn load_hashes(watch_dir: &Path, hashes_path: &Path) -> Result<HashMap<String, String>> {
    match File::open(hashes_path) {
        Ok(file) => {
            let reader = BufReader::new(file);
            let mut hashes = HashMap::new();
            for line in reader.lines() {
                let line = line.context("讀取 hashes.txt 失敗")?;
                if let Some((path, hash)) = parse_hash_line(&line) {
                    hashes.insert(path, hash);
                }
            }
            Ok(hashes)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            scan_directory(watch_dir, hashes_path)
        }
        Err(e) => Err(e).context("無法開啟 hashes.txt"),
    }
}

/// 解析雜湊紀錄行：新格式為 tab 分隔，舊格式為第一個冒號分隔（向下相容）
fn parse_hash_line(line: &str) -> Option<(String, String)> {
    if let Some((path, hash)) = line.split_once('\t') {
        if !path.is_empty() && !hash.is_empty() {
            return Some((path.to_string(), hash.to_string()));
        }
    }
    let mut parts = line.splitn(2, ':');
    match (parts.next(), parts.next()) {
        (Some(path), Some(hash)) if !path.is_empty() && !hash.is_empty() => {
            Some((path.to_string(), hash.to_string()))
        }
        _ => None,
    }
}

/// 遞迴掃描監控目錄內所有檔案（與 watcher 的 Recursive 範圍一致）
fn scan_directory(watch_dir: &Path, hashes_path: &Path) -> Result<HashMap<String, String>> {
    let mut hashes = HashMap::new();
    scan_directory_inner(watch_dir, hashes_path, &mut hashes)?;
    Ok(hashes)
}

fn scan_directory_inner(
    dir: &Path,
    hashes_path: &Path,
    hashes: &mut HashMap<String, String>,
) -> Result<()> {
    for entry in fs::read_dir(dir)
        .with_context(|| format!("無法讀取目錄: {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();

        if is_hashes_file(&path, hashes_path) {
            continue;
        }

        if path.is_dir() {
            scan_directory_inner(&path, hashes_path, hashes)?;
        } else if path.is_file() {
            let key = path_key(&path)?;
            let hash = calculate_hash(&path)?;
            hashes.insert(key, hash);
        }
    }
    Ok(())
}

fn write_hashes(hashes: &HashMap<String, String>, hashes_path: &Path) -> Result<()> {
    let file = File::create(hashes_path)
        .with_context(|| format!("無法寫入 {}", hashes_path.display()))?;
    let mut writer = BufWriter::new(file);

    for (path, hash) in hashes {
        // tab 分隔避免路徑中的冒號造成解析錯誤
        writeln!(writer, "{path}\t{hash}")
            .context("寫入 hashes.txt 失敗")?;
    }
    Ok(())
}

fn calculate_hash(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("無法開啟檔案: {}", path.display()))?;
    let mut sha256 = Sha256::new();
    std::io::copy(&mut file, &mut sha256)
        .with_context(|| format!("無法讀取檔案: {}", path.display()))?;
    Ok(hex::encode(sha256.finalize()))
}

/// 處理新增或修改；回傳 true 表示雜湊表有變動
fn handle_file_change(
    hashes: &mut HashMap<String, String>,
    path: &Path,
    notify_email: &str,
    email_tx: &Sender<EmailJob>,
) -> Result<bool> {
    let path_str = path_key(path)?;
    let new_hash = calculate_hash(path)?;

    if hashes.get(&path_str).is_some_and(|old| old == &new_hash) {
        return Ok(false);
    }

    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
    let body = match hashes.get(&path_str) {
        Some(old_hash) => format!(
            "檔案變動時間: {timestamp}\n路徑: {path_str}\n原始雜湊值: {old_hash}\n新的雜湊值: {new_hash}"
        ),
        None => format!(
            "檔案變動時間: {timestamp}\n路徑: {path_str}\n新的雜湊值: {new_hash}"
        ),
    };

    hashes.insert(path_str, new_hash);
    queue_email(email_tx, notify_email, body)?;
    Ok(true)
}

/// 處理刪除；回傳 true 表示雜湊表有變動
fn handle_file_remove(
    hashes: &mut HashMap<String, String>,
    path: &Path,
    notify_email: &str,
    email_tx: &Sender<EmailJob>,
) -> Result<bool> {
    let Some(key) = find_hash_key(hashes, path) else {
        return Ok(false);
    };
    let Some(old_hash) = hashes.remove(&key) else {
        return Ok(false);
    };

    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
    let body = format!(
        "檔案刪除時間: {timestamp}\n路徑: {key}\n刪除前雜湊值: {old_hash}"
    );
    queue_email(email_tx, notify_email, body)?;
    Ok(true)
}

/// 在雜湊表中尋找與事件路徑對應的 key（刪除時路徑可能無法 canonicalize）
fn find_hash_key(hashes: &HashMap<String, String>, path: &Path) -> Option<String> {
    let lossy = path.to_string_lossy().into_owned();
    if hashes.contains_key(&lossy) {
        return Some(lossy);
    }
    if let Ok(canonical) = path_key(path) {
        if hashes.contains_key(&canonical) {
            return Some(canonical);
        }
    }
    hashes
        .keys()
        .find(|k| Path::new(k.as_str()) == path)
        .cloned()
        .or_else(|| {
            let name = path.file_name()?;
            hashes
                .keys()
                .find(|k| Path::new(k.as_str()).file_name() == Some(name))
                .cloned()
        })
}

fn queue_email(email_tx: &Sender<EmailJob>, to: &str, body: String) -> Result<()> {
    email_tx
        .send(EmailJob {
            to: to.to_string(),
            body,
        })
        .context("郵件佇列已關閉")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{DataChange, ModifyKind, RemoveKind};
    use notify::{Event, EventKind};
    use notify_debouncer_full::DebouncedEvent;
    use std::sync::mpsc::{self, Receiver};
    use std::time::Instant;
    use tempfile::tempdir;

    fn key(path: &Path) -> String {
        path.canonicalize()
            .unwrap_or_else(|_| path.to_path_buf())
            .to_string_lossy()
            .into_owned()
    }

    fn debounced_event(kind: EventKind, paths: &[&Path]) -> DebouncedEvent {
        let event = paths.iter().fold(Event::new(kind), |event, path| {
            event.add_path((*path).to_path_buf())
        });
        DebouncedEvent::new(event, Instant::now())
    }

    fn queued_jobs(rx: &Receiver<EmailJob>) -> Vec<EmailJob> {
        rx.try_iter().collect()
    }

    #[test]
    fn parse_hash_line_supports_tab_and_legacy_formats() {
        assert_eq!(
            parse_hash_line("/tmp/a:b.txt\t012345"),
            Some(("/tmp/a:b.txt".to_string(), "012345".to_string()))
        );
        assert_eq!(
            parse_hash_line("/tmp/a:b.txt:012345"),
            Some(("/tmp/a".to_string(), "b.txt:012345".to_string()))
        );
        assert_eq!(parse_hash_line("invalid"), None);
        assert_eq!(parse_hash_line("\t012345"), None);
    }

    #[test]
    fn scan_directory_and_hashes_round_trip() {
        let temp = tempdir().unwrap();
        let nested = temp.path().join("nested");
        fs::create_dir(&nested).unwrap();
        let file = nested.join("data.txt");
        fs::write(&file, "initial").unwrap();
        let hashes_path = temp.path().join(HASHES_FILENAME);

        let hashes = scan_directory(temp.path(), &hashes_path).unwrap();

        assert_eq!(
            hashes.get(&key(&file)),
            Some(&calculate_hash(&file).unwrap())
        );
        assert!(!hashes.contains_key(&key(&hashes_path)));

        write_hashes(&hashes, &hashes_path).unwrap();
        assert_eq!(load_hashes(temp.path(), &hashes_path).unwrap(), hashes);
    }

    #[test]
    fn process_file_lifecycle_updates_hashes_and_queues_notifications() {
        let temp = tempdir().unwrap();
        let file = temp.path().join("watched.txt");
        let hashes_path = temp.path().join(HASHES_FILENAME);
        let (tx, rx) = mpsc::channel();
        let mut hashes = HashMap::new();

        fs::write(&file, "one").unwrap();
        process_debounced_event(
            &debounced_event(EventKind::Create(notify::event::CreateKind::File), &[&file]),
            temp.path(),
            &hashes_path,
            &mut hashes,
            "to@example.com",
            &tx,
        )
        .unwrap();
        assert_eq!(
            hashes.get(&key(&file)),
            Some(&calculate_hash(&file).unwrap())
        );
        assert_eq!(queued_jobs(&rx).len(), 1);
        assert_eq!(load_hashes(temp.path(), &hashes_path).unwrap(), hashes);

        fs::write(&file, "two").unwrap();
        process_debounced_event(
            &debounced_event(
                EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                &[&file],
            ),
            temp.path(),
            &hashes_path,
            &mut hashes,
            "to@example.com",
            &tx,
        )
        .unwrap();
        assert_eq!(queued_jobs(&rx).len(), 1);

        let unchanged = hashes.clone();
        process_debounced_event(
            &debounced_event(
                EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                &[&file],
            ),
            temp.path(),
            &hashes_path,
            &mut hashes,
            "to@example.com",
            &tx,
        )
        .unwrap();
        assert_eq!(hashes, unchanged);
        assert!(queued_jobs(&rx).is_empty());

        fs::remove_file(&file).unwrap();
        process_debounced_event(
            &debounced_event(EventKind::Remove(RemoveKind::File), &[&file]),
            temp.path(),
            &hashes_path,
            &mut hashes,
            "to@example.com",
            &tx,
        )
        .unwrap();
        assert!(!hashes.contains_key(&key(&file)));
        assert_eq!(queued_jobs(&rx).len(), 1);
        assert_eq!(load_hashes(temp.path(), &hashes_path).unwrap(), hashes);
    }
}

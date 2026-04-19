
use std::collections::HashMap;
use std::env;
use std::fs::{read_dir, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::thread;
use chrono::Local;
use lettre::{Message, SmtpTransport, Transport};
use lettre::transport::smtp::authentication::Credentials;
use notify::{RecursiveMode, Watcher};
use sha2::{Sha256, Digest};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("使用方式: {} <監控目錄> <通知電子郵件>", args[0]);
        return;
    }
    let dir_path = &args[1];
    let notify_email = &args[2];

    let mut file_hashes = load_hashes(&dir_path);
    let hashes_path = Path::new(dir_path).join("hashes.txt");
    write_hashes(&file_hashes, &dir_path);

    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = notify::recommended_watcher(tx).unwrap();
    watcher.watch(Path::new(dir_path), RecursiveMode::Recursive).unwrap();

    loop {
        match rx.recv() {
            Ok(Ok(event)) => {
                if event.paths.iter().all(|p| p == &hashes_path) {
                    continue;
                }
                match event.kind {
                    notify::EventKind::Create(_) | notify::EventKind::Modify(_) => {
                        let mut changed = false;
                        for path in &event.paths {
                            if path.is_file() && path != &hashes_path {
                                handle_file_change(&mut file_hashes, path, notify_email);
                                changed = true;
                            }
                        }
                        if changed {
                            write_hashes(&file_hashes, &dir_path);
                        }
                    }
                    notify::EventKind::Remove(_) => {
                        for path in &event.paths {
                            if path == &hashes_path {
                                continue;
                            }
                            let path_str = path.to_string_lossy().to_string();
                            if file_hashes.remove(&path_str).is_some() {
                                write_hashes(&file_hashes, &dir_path);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(Err(e)) => eprintln!("監視錯誤: {:?}", e),
            Err(e) => eprintln!("接收錯誤: {:?}", e),
        }
    }
}

fn load_hashes(dir_path: &str) -> HashMap<String, String> {
    let mut hashes = HashMap::new();
    let hashes_file = Path::new(dir_path).join("hashes.txt");

    match File::open(&hashes_file) {
        Ok(file) => {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line.unwrap();
                let mut parts = line.splitn(2, ':');
                if let (Some(path), Some(hash)) = (parts.next(), parts.next()) {
                    hashes.insert(path.to_string(), hash.to_string());
                }
            }
        }
        Err(_) => {
            let entries = read_dir(dir_path).unwrap();
            for entry in entries {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_file() {
                    let hash = calculate_hash(&path);
                    hashes.insert(path.to_string_lossy().to_string(), hash);
                }
            }
        }
    }

    hashes
}

fn write_hashes(hashes: &HashMap<String, String>, dir_path: &str) {
    let hashes_file = Path::new(dir_path).join("hashes.txt");
    let file = File::create(hashes_file).unwrap();
    let mut writer = BufWriter::new(file);

    for (path, hash) in hashes {
        writeln!(writer, "{}:{}", path, hash).unwrap();
    }
}

fn calculate_hash<P: AsRef<Path>>(path: P) -> String {
    let mut file = File::open(path).unwrap();
    let mut sha256 = Sha256::new();
    std::io::copy(&mut file, &mut sha256).unwrap();
    hex::encode(sha256.finalize())
}

fn handle_file_change(
    hashes: &mut HashMap<String, String>,
    path: &Path,
    notify_email: &str,
) {
    let path_str = path.to_string_lossy().to_string();
    let new_hash = calculate_hash(path);

    if let Some(old_hash) = hashes.get(&path_str) {
        if old_hash == &new_hash {
            return;
        }
    }

    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
    let email_body = match hashes.get(&path_str) {
        Some(old_hash) => format!(
            "檔案變動時間: {}\n路徑: {}\n原始雜湊值: {}\n新的雜湊值: {}",
            timestamp, path_str, old_hash, new_hash
        ),
        None => format!(
            "檔案變動時間: {}\n路徑: {}\n新的雜湊值: {}",
            timestamp, path_str, new_hash
        ),
    };

    hashes.insert(path_str, new_hash);
    let email = notify_email.to_string();
    thread::spawn(move || {
        send_notification_email(&email, &email_body);
    });
}

fn send_notification_email(to_email: &str, body: &str) {
    let email = Message::builder()
        .from("監控程式 <monitor@example.com>".parse().unwrap())
        .to(to_email.parse().unwrap())
        .subject("檔案變動通知")
        .body(body.to_string())
        .unwrap();

    let creds = Credentials::new("smtp_username".to_string(), "smtp_password".to_string());

    let mailer = SmtpTransport::relay("smtp.example.com")
        .unwrap()
        .credentials(creds)
        .build();

    match mailer.send(&email) {
        Ok(_) => println!("電子郵件已成功發送!"),
        Err(e) => eprintln!("無法發送電子郵件: {:?}", e),
    }
}
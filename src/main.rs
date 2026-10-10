//! Réveilleur : proxy Minecraft minimaliste devant un serveur géré par AMP.
//!
//! - Serveur prêt      : la connexion est relayée telle quelle (aucun décodage).
//! - Serveur endormi   : le joueur est envoyé dans un limbo (PicoLimbo), le serveur
//!                       est démarré via l'API AMP, puis le joueur reçoit un paquet
//!                       « Transfer » qui le reconnecte tout seul au vrai serveur.
//! - Plus personne     : après `idle_stop_minutes`, le serveur est arrêté via AMP.

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Deserialize)]
struct Config {
    /// Adresse publique écoutée (ex. "0.0.0.0:25566").
    listen: String,
    /// Vrai serveur Minecraft (ex. "127.0.0.1:25567").
    backend: String,
    /// Serveur limbo (PicoLimbo, ex. "127.0.0.1:30066").
    limbo: String,
    /// API ADS (ex. "http://127.0.0.1:8080/API/").
    ads_url: String,
    /// Identifiant (GUID) et nom de l'instance AMP.
    instance_id: String,
    instance_name: String,
    amp_user: String,
    /// Fichier contenant le mot de passe du compte AMP (lisible par root seulement).
    amp_password_file: String,
    /// Arrêt après N minutes sans joueur (0 = jamais).
    #[serde(default)]
    idle_stop_minutes: u64,
    /// Délai après l'état « Ready » d'AMP avant de transférer les joueurs.
    #[serde(default = "default_grace")]
    ready_grace_seconds: u64,
    /// Décompte annoncé dans le chat avant le transfert (« Connexion dans 3… »).
    #[serde(default = "default_countdown")]
    countdown_seconds: u64,
    /// Fichier où sont mémorisées les dernières durées de démarrage (secondes).
    startup_history_file: String,
    /// Numéros des paquets clientbound play utilisés, par version de protocole.
    /// Obtenus via le générateur de rapports du serveur (packets.json).
    protocols: HashMap<String, ProtocolIds>,
}

#[derive(Deserialize, Clone, Copy)]
struct ProtocolIds {
    transfer: i32,
    system_chat: i32,
}

fn default_grace() -> u64 {
    1
}

fn default_countdown() -> u64 {
    3
}

/// Paliers (secondes restantes estimées) annoncés dans le chat pendant l'attente.
const MILESTONES: [u64; 7] = [30, 20, 10, 5, 3, 2, 1];

/// Nombre de démarrages pris en compte dans la moyenne.
const HISTORY_LEN: usize = 10;

struct Shared {
    cfg: Config,
    ready: watch::Receiver<bool>,
    wake: AtomicBool,
    /// Moment où le réveil a été demandé (pour le décompte et les statistiques).
    wake_since: Mutex<Option<Instant>>,
    /// Durée de démarrage estimée (moyenne des derniers démarrages), en secondes.
    estimate: AtomicUsize,
    /// Réveille immédiatement la boucle de supervision.
    nudge: Mutex<std::sync::mpsc::Sender<()>>,
    players: AtomicUsize,
    last_activity: Mutex<Instant>,
}

impl Shared {
    fn touch(&self) {
        *self.last_activity.lock().unwrap() = Instant::now();
    }
}

fn load_history(path: &str) -> Vec<u64> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .split_whitespace()
        .filter_map(|s| s.parse().ok())
        .collect()
}

fn average(h: &[u64]) -> usize {
    if h.is_empty() {
        30
    } else {
        (h.iter().sum::<u64>() as f64 / h.len() as f64).ceil() as usize
    }
}

fn log(msg: impl AsRef<str>) {
    // journald horodate déjà ; on garde des lignes courtes.
    eprintln!("{}", msg.as_ref());
}

// ---------------------------------------------------------------- protocole

fn read_varint(buf: &[u8]) -> Option<(i32, usize)> {
    let mut val: u32 = 0;
    for i in 0..5 {
        let b = *buf.get(i)?;
        val |= ((b & 0x7F) as u32) << (7 * i);
        if b & 0x80 == 0 {
            return Some((val as i32, i + 1));
        }
    }
    None
}

fn write_varint(out: &mut Vec<u8>, v: i32) {
    let mut v = v as u32;
    loop {
        if v & !0x7F == 0 {
            out.push(v as u8);
            return;
        }
        out.push((v as u8 & 0x7F) | 0x80);
        v >>= 7;
    }
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    write_varint(out, s.len() as i32);
    out.extend_from_slice(s.as_bytes());
}

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 5);
    write_varint(&mut out, payload.len() as i32);
    out.extend_from_slice(payload);
    out
}

struct Handshake {
    raw: Vec<u8>,
    protocol: i32,
    host: String,
    port: u16,
    intent: i32,
}

impl Handshake {
    /// Même poignée de main, mais avec une autre intention (3 = transfer -> 2 = login).
    fn with_intent(&self, intent: i32) -> Vec<u8> {
        let mut p = Vec::new();
        write_varint(&mut p, 0);
        write_varint(&mut p, self.protocol);
        write_string(&mut p, &self.host);
        p.extend_from_slice(&self.port.to_be_bytes());
        write_varint(&mut p, intent);
        frame(&p)
    }
}

async fn read_handshake(s: &mut TcpStream) -> Result<Option<Handshake>> {
    // Longueur (VarInt) octet par octet, pour ne rien consommer au-delà.
    let mut len_bytes = Vec::new();
    loop {
        let b = s.read_u8().await?;
        if len_bytes.is_empty() && b == 0xFE {
            return Ok(None); // ancien ping (< 1.7)
        }
        len_bytes.push(b);
        if b & 0x80 == 0 {
            break;
        }
        if len_bytes.len() >= 3 {
            bail!("handshake trop long");
        }
    }
    let (len, _) = read_varint(&len_bytes).ok_or_else(|| anyhow!("varint"))?;
    if !(1..=1024).contains(&len) {
        bail!("taille de handshake invalide");
    }
    let mut body = vec![0u8; len as usize];
    s.read_exact(&mut body).await?;

    let mut i = 0;
    let (id, n) = read_varint(&body[i..]).ok_or_else(|| anyhow!("id"))?;
    i += n;
    if id != 0 {
        bail!("premier paquet non-handshake");
    }
    let (protocol, n) = read_varint(&body[i..]).ok_or_else(|| anyhow!("protocole"))?;
    i += n;
    let (slen, n) = read_varint(&body[i..]).ok_or_else(|| anyhow!("hôte"))?;
    i += n;
    let host = String::from_utf8_lossy(body.get(i..i + slen as usize).ok_or_else(|| anyhow!("hôte"))?)
        .to_string();
    i += slen as usize;
    let port = u16::from_be_bytes([*body.get(i).ok_or_else(|| anyhow!("port"))?, *body.get(i + 1).ok_or_else(|| anyhow!("port"))?]);
    i += 2;
    let (intent, _) = read_varint(&body[i..]).ok_or_else(|| anyhow!("intention"))?;

    let mut raw = len_bytes;
    raw.extend_from_slice(&body);
    Ok(Some(Handshake { raw, protocol, host, port, intent }))
}

/// Chaîne NBT (UTF-8 modifié de Java) : on écarte les caractères hors BMP (emojis)
/// et le caractère nul, qui s'encodent différemment et font planter le client.
fn write_nbt_string(out: &mut Vec<u8>, s: &str) {
    let clean: String = s.chars().filter(|c| (*c as u32) <= 0xFFFF && *c != '\0').collect();
    out.extend_from_slice(&(clean.len() as u16).to_be_bytes());
    out.extend_from_slice(clean.as_bytes());
}

/// Paquet « system_chat » : texte coloré dans le chat (ou au-dessus de la barre d'inventaire).
fn chat_packet(id: i32, text: &str, color: &str, action_bar: bool) -> Vec<u8> {
    let mut p = Vec::new();
    write_varint(&mut p, id);
    p.push(0x0A); // compound racine (NBT réseau : sans nom)
    for (k, v) in [("color", color), ("text", text)] {
        p.push(0x08); // tag string
        write_nbt_string(&mut p, k);
        write_nbt_string(&mut p, v);
    }
    p.push(0x00); // fin du compound
    p.push(action_bar as u8);
    frame(&p)
}

/// Nom d'hôte « propre » pour le paquet Transfer (sans marqueurs Forge, sans point final).
fn clean_host(h: &str) -> String {
    h.split('\0').next().unwrap_or("").trim_end_matches('.').to_string()
}

// ---------------------------------------------------------------- relais

async fn pipe(client: TcpStream, target: &str, first: &[u8]) -> Result<()> {
    let mut up = TcpStream::connect(target).await.with_context(|| format!("connexion à {target}"))?;
    up.set_nodelay(true).ok();
    up.write_all(first).await?;
    let mut client = client;
    tokio::io::copy_bidirectional(&mut client, &mut up).await.ok();
    Ok(())
}

#[derive(PartialEq, Clone, Copy)]
enum Phase {
    Login,
    Config,
    Play,
}

/// Fait patienter le joueur dans le limbo, puis lui envoie un paquet Transfer.
async fn limbo_hold(client: TcpStream, hs: Handshake, sh: Arc<Shared>) -> Result<()> {
    let mut up = TcpStream::connect(&sh.cfg.limbo).await.context("connexion au limbo")?;
    up.set_nodelay(true).ok();
    up.write_all(&hs.with_intent(2)).await?;

    let ids = sh.cfg.protocols.get(&hs.protocol.to_string()).copied();
    let (mut cr, mut cw) = client.into_split();
    let (mut lr, mut lw) = up.into_split();
    let c2l = tokio::spawn(async move {
        tokio::io::copy(&mut cr, &mut lw).await.ok();
    });

    let arrived = Instant::now();
    let mut ready = sh.ready.clone();
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = vec![0u8; 8192];
    let mut phase = Phase::Login;
    let mut announced = false;
    let mut next_milestone = u64::MAX;
    let mut late_notice = false;
    let result = loop {
        if phase == Phase::Play {
            let Some(ids) = ids else {
                break Err(anyhow!("protocole {} inconnu : pas de transfert possible", hs.protocol));
            };
            let est = sh.estimate.load(Ordering::SeqCst) as u64;
            if !announced {
                let msg = format!("Démarrage du serveur en cours, environ {est} s. Ne te déconnecte pas !");
                cw.write_all(&chat_packet(ids.system_chat, &msg, "gold", false)).await?;
                announced = true;
            }
            if *ready.borrow() {
                // Serveur prêt : on annonce et on transfère tout de suite.
                cw.write_all(&chat_packet(ids.system_chat, "Serveur prêt, connexion !", "green", false)).await?;
                let host = clean_host(&hs.host);
                let mut p = Vec::new();
                write_varint(&mut p, ids.transfer);
                write_string(&mut p, &host);
                write_varint(&mut p, hs.port as i32);
                cw.write_all(&frame(&p)).await?;
                cw.flush().await?;
                log(format!("transfert vers {host}:{} après {} s d'attente", hs.port, arrived.elapsed().as_secs()));
                tokio::time::sleep(Duration::from_secs(2)).await;
                break Ok(());
            }
            // En attente : un message par palier franchi (30, 20, 10, 5, 3, 2, 1 s).
            let since = sh.wake_since.lock().unwrap().unwrap_or(arrived);
            let elapsed = since.elapsed().as_secs();
            let remaining = est.saturating_sub(elapsed);
            if let Some(&m) = MILESTONES.iter().find(|&&m| remaining == m && m < next_milestone && m < est) {
                cw.write_all(&chat_packet(ids.system_chat, &format!("Connexion dans ~{m} s"), "yellow", false)).await?;
                next_milestone = m;
            }
            if remaining == 0 && !late_notice {
                cw.write_all(&chat_packet(ids.system_chat, "Presque prêt…", "yellow", false)).await?;
                late_notice = true;
            }
        }
        tokio::select! {
            n = lr.read(&mut chunk) => {
                let n = n?;
                if n == 0 { break Ok(()); }
                buf.extend_from_slice(&chunk[..n]);
                // On ne relaie que des trames complètes, pour pouvoir insérer
                // le paquet Transfer entre deux trames.
                loop {
                    let Some((len, ln)) = read_varint(&buf) else { break };
                    if len < 0 || len > (1 << 21) { break_with_error()?; }
                    let total = ln + len as usize;
                    if buf.len() < total { break; }
                    if let Some((id, _)) = read_varint(&buf[ln..total]) {
                        phase = match (phase, id) {
                            (Phase::Login, 2) => Phase::Config,  // login_finished
                            (Phase::Config, 3) => Phase::Play,   // finish_configuration
                            (p, _) => p,
                        };
                    }
                    cw.write_all(&buf[..total]).await?;
                    buf.drain(..total);
                }
            }
            _ = ready.changed() => {}
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    };
    c2l.abort();
    result
}

fn break_with_error() -> Result<()> {
    bail!("trame limbo invalide")
}

async fn handle(mut client: TcpStream, sh: Arc<Shared>) -> Result<()> {
    client.set_nodelay(true).ok();
    let peer = client.peer_addr().map(|a| a.ip().to_string()).unwrap_or_default();
    let hs = match tokio::time::timeout(Duration::from_secs(10), read_handshake(&mut client)).await?? {
        Some(h) => h,
        None => return Ok(()), // ancien ping : ignoré
    };
    let ready = *sh.ready.borrow();

    if hs.intent == 1 {
        // Liste des serveurs : le vrai serveur s'il est prêt, sinon la MOTD « en veille » du limbo.
        let target = if ready { &sh.cfg.backend } else { &sh.cfg.limbo };
        return pipe(client, target, &hs.raw).await;
    }

    if ready {
        if let Ok(up) = TcpStream::connect(&sh.cfg.backend).await {
            drop(up);
            sh.players.fetch_add(1, Ordering::SeqCst);
            sh.touch();
            log(format!("{peer} -> serveur ({} en ligne)", sh.players.load(Ordering::SeqCst)));
            let r = pipe(client, &sh.cfg.backend, &hs.raw).await;
            sh.players.fetch_sub(1, Ordering::SeqCst);
            sh.touch();
            return r;
        }
    }

    log(format!("{peer} -> limbo (serveur endormi, réveil demandé)"));
    if !sh.wake.swap(true, Ordering::SeqCst) {
        *sh.wake_since.lock().unwrap() = Some(Instant::now());
    }
    sh.nudge.lock().unwrap().send(()).ok();
    sh.touch();
    limbo_hold(client, hs, sh).await
}

// ---------------------------------------------------------------- AMP

struct Amp {
    url: String,
    user: String,
    password: String,
    session: Option<String>,
}

impl Amp {
    fn raw(&self, method: &str, mut body: serde_json::Value) -> Result<serde_json::Value> {
        if let Some(s) = &self.session {
            body["SESSIONID"] = serde_json::Value::String(s.clone());
        }
        let v: serde_json::Value = ureq::post(&format!("{}{}", self.url, method))
            .set("Accept", "application/json")
            .timeout(Duration::from_secs(20))
            .send_json(body)?
            .into_json()?;
        Ok(v)
    }

    fn login(&mut self) -> Result<()> {
        self.session = None;
        let v = self.raw(
            "Core/Login",
            serde_json::json!({"username": self.user, "password": self.password, "token": "", "rememberMe": false}),
        )?;
        let sid = v["sessionID"].as_str().ok_or_else(|| anyhow!("login AMP refusé"))?;
        self.session = Some(sid.to_string());
        Ok(())
    }

    fn call(&mut self, method: &str) -> Result<serde_json::Value> {
        self.call_with(method, serde_json::json!({}))
    }

    fn call_with(&mut self, method: &str, body: serde_json::Value) -> Result<serde_json::Value> {
        if self.session.is_none() {
            self.login()?;
        }
        let mut v = self.raw(method, body.clone())?;
        // Session expirée : AMP renvoie un objet d'erreur ou un statut false.
        if v.get("Title").is_some() || v == serde_json::Value::Bool(false) {
            self.login()?;
            v = self.raw(method, body)?;
            if let Some(t) = v.get("Title") {
                bail!("{t} : {}", v["Message"]);
            }
        }
        // Action refusée (permission manquante, etc.).
        if v.get("Status") == Some(&serde_json::Value::Bool(false)) {
            bail!("{method} refusé : {}", v["Reason"]);
        }
        Ok(v)
    }

    fn state(&mut self) -> Result<i64> {
        let v = self.call("Core/GetStatus")?;
        v["State"].as_i64().ok_or_else(|| anyhow!("état AMP illisible : {v}"))
    }
}

/// Boucle de supervision : suit l'état AMP, démarre sur demande, arrête si inactif.
fn supervisor(sh: Arc<Shared>, ready_tx: watch::Sender<bool>, nudge: std::sync::mpsc::Receiver<()>) {
    let password = std::fs::read_to_string(&sh.cfg.amp_password_file)
        .expect("mot de passe AMP illisible")
        .trim()
        .to_string();
    // Instance (via le proxy ADS) et ADS lui-même, avec le même compte.
    let mut amp = Amp {
        url: format!("{}ADSModule/Servers/{}/API/", sh.cfg.ads_url, sh.cfg.instance_id),
        user: sh.cfg.amp_user.clone(),
        password: password.clone(),
        session: None,
    };
    let mut ads = Amp { url: sh.cfg.ads_url.clone(), user: sh.cfg.amp_user.clone(), password, session: None };
    let mut ready_since: Option<Instant> = None;
    let mut last_start_cmd: Option<Instant> = None;
    let mut last_ads_start: Option<Instant> = None;
    loop {
        let waking = sh.wake.load(Ordering::SeqCst);
        match amp.state() {
            Ok(st) => {
                // 20 = Ready ; 0 = Stopped ; 100 = Failed
                if st == 20 {
                    let since = *ready_since.get_or_insert_with(Instant::now);
                    let ok = since.elapsed() >= Duration::from_secs(sh.cfg.ready_grace_seconds);
                    ready_tx.send_if_modified(|r| {
                        let changed = *r != ok;
                        *r = ok;
                        changed
                    });
                    if ok && waking {
                        sh.wake.store(false, Ordering::SeqCst);
                        // Mémorise la durée réelle du démarrage pour affiner l'estimation.
                        if let Some(t0) = sh.wake_since.lock().unwrap().take() {
                            let secs = t0.elapsed().as_secs();
                            let path = &sh.cfg.startup_history_file;
                            let mut h = load_history(path);
                            h.push(secs);
                            if h.len() > HISTORY_LEN {
                                h.drain(..h.len() - HISTORY_LEN);
                            }
                            let txt: Vec<String> = h.iter().map(|s| s.to_string()).collect();
                            std::fs::write(path, txt.join(" ") + "\n").ok();
                            sh.estimate.store(average(&h), Ordering::SeqCst);
                            log(format!("serveur prêt en {secs} s (moyenne {} s), transfert des joueurs en attente", average(&h)));
                        } else {
                            log("serveur prêt, transfert des joueurs en attente");
                        }
                    }
                } else {
                    ready_since = None;
                    ready_tx.send_if_modified(|r| std::mem::replace(r, false));
                }

                let recently = last_start_cmd.map_or(false, |t| t.elapsed() < Duration::from_secs(60));
                if waking && (st == 0 || st == 100) && !recently {
                    log(format!("démarrage du serveur (état AMP {st})"));
                    match amp.call("Core/Start") {
                        Ok(_) => last_start_cmd = Some(Instant::now()),
                        Err(e) => log(format!("échec Core/Start : {e:#}")),
                    }
                }

                let idle = sh.cfg.idle_stop_minutes;
                if idle > 0 && st == 20 && !waking && sh.players.load(Ordering::SeqCst) == 0 {
                    let quiet = sh.last_activity.lock().unwrap().elapsed();
                    if quiet >= Duration::from_secs(idle * 60) {
                        log(format!("aucun joueur depuis {idle} min : arrêt du serveur"));
                        if let Err(e) = amp.call("Core/Stop") {
                            log(format!("échec Core/Stop : {e:#}"));
                        }
                        sh.touch();
                    }
                }
            }
            Err(e) => {
                ready_since = None;
                ready_tx.send_if_modified(|r| std::mem::replace(r, false));
                amp.session = None;
                // L'instance AMP elle-même (son conteneur) est arrêtée : on la démarre via ADS.
                let recently = last_ads_start.map_or(false, |t| t.elapsed() < Duration::from_secs(90));
                if waking && !recently {
                    log(format!("instance AMP indisponible ({e:#}) : démarrage via ADS"));
                    let body = serde_json::json!({"InstanceName": sh.cfg.instance_name});
                    if let Err(e) = ads.call_with("ADSModule/StartInstance", body) {
                        log(format!("échec ADSModule/StartInstance : {e:#}"));
                    }
                    last_ads_start = Some(Instant::now());
                } else if !waking {
                    log(format!("API AMP indisponible : {e:#}"));
                }
            }
        }
        // Au repos on vérifie toutes les 10 s, mais une demande de réveil interrompt l'attente.
        let pause = if sh.wake.load(Ordering::SeqCst) { 1 } else { 10 };
        let _ = nudge.recv_timeout(Duration::from_secs(pause));
        while nudge.try_recv().is_ok() {}
    }
}

// ---------------------------------------------------------------- main

fn main() -> Result<()> {
    let path = std::env::args().nth(1).unwrap_or_else(|| "/etc/reveilleur/coccinelles.toml".into());
    let cfg: Config = toml::from_str(&std::fs::read_to_string(&path).with_context(|| path.clone())?)?;
    let (ready_tx, ready_rx) = watch::channel(false);
    let (nudge_tx, nudge_rx) = std::sync::mpsc::channel();
    let estimate = average(&load_history(&cfg.startup_history_file));
    let sh = Arc::new(Shared {
        cfg,
        ready: ready_rx,
        wake: AtomicBool::new(false),
        wake_since: Mutex::new(None),
        estimate: AtomicUsize::new(estimate),
        nudge: Mutex::new(nudge_tx),
        players: AtomicUsize::new(0),
        last_activity: Mutex::new(Instant::now()),
    });

    let sup = sh.clone();
    std::thread::spawn(move || supervisor(sup, ready_tx, nudge_rx));

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(async move {
        let listener = TcpListener::bind(&sh.cfg.listen).await?;
        log(format!("réveilleur en écoute sur {}", sh.cfg.listen));
        loop {
            let (sock, _) = listener.accept().await?;
            let sh = sh.clone();
            tokio::spawn(async move {
                if let Err(e) = handle(sock, sh).await {
                    log(format!("connexion : {e:#}"));
                }
            });
        }
    })
}

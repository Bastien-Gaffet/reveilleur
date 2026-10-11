//! Réveilleur : proxy Minecraft minimaliste devant un ou plusieurs serveurs gérés par AMP.
//!
//! - Serveur prêt    : la connexion est relayée telle quelle (aucun décodage).
//! - Serveur endormi : le joueur Java patiente dans un limbo partagé (PicoLimbo), le
//!                     serveur est démarré via l'API AMP, puis le joueur reçoit un
//!                     paquet « Transfer » qui le reconnecte tout seul. Un joueur
//!                     Bedrock (via Geyser) reçoit un message « reconnecte-toi ».
//! - Plus personne   : après `idle_stop_minutes`, le serveur est arrêté via AMP.
//!
//! Un seul processus et un seul limbo pour tous les serveurs : l'aiguillage se fait
//! selon le port d'entrée puis l'adresse demandée par le client.

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

// ---------------------------------------------------------------- configuration

#[derive(Deserialize)]
struct Config {
    /// Serveur limbo partagé (PicoLimbo, ex. "127.0.0.1:30066").
    limbo: String,
    /// API ADS (ex. "http://127.0.0.1:8080/API/"). Inutile si tous les serveurs sont en systemd.
    #[serde(default)]
    ads_url: String,
    #[serde(default)]
    amp_user: String,
    /// Fichier contenant le mot de passe du compte AMP (lisible par root seulement).
    #[serde(default)]
    amp_password_file: String,
    /// Numéros des paquets clientbound play utilisés, par version de protocole.
    /// Obtenus via le générateur de rapports du serveur (packets.json).
    #[serde(default)]
    protocols: HashMap<String, ProtocolIds>,
    /// Fichier complémentaire de numéros de paquets, relu à chaud sur SIGHUP
    /// (alimenté par l'outil de mise à jour assistée, après validation en jeu).
    #[serde(default)]
    protocols_file: Option<String>,
    servers: Vec<ServerCfg>,
    listeners: Vec<ListenerCfg>,
}

#[derive(Deserialize, Clone, Copy, Debug)]
struct ProtocolIds {
    transfer: i32,
    system_chat: i32,
}

#[derive(Deserialize)]
struct ListenerCfg {
    /// Adresse écoutée (ex. "0.0.0.0:25565").
    addr: String,
    /// Serveurs joignables par ce port ; le premier est celui par défaut,
    /// les autres sont choisis selon l'adresse demandée (`hostnames`).
    servers: Vec<String>,
    /// Les connexions arrivent précédées d'un en-tête PROXY (nginx, Geyser).
    #[serde(default)]
    proxy_protocol_in: bool,
    /// Connexions Bedrock via Geyser : pas de limbo ni de transfert possible.
    #[serde(default)]
    bedrock: bool,
}

#[derive(Deserialize)]
struct ServerCfg {
    /// Identifiant interne (ex. "pcsi").
    name: String,
    /// Nom affiché aux joueurs (ex. "PCSI").
    display_name: String,
    /// Adresses qui désignent ce serveur (comparées sans casse).
    #[serde(default)]
    hostnames: Vec<String>,
    /// Vrai serveur, en local uniquement (ex. "127.0.0.1:25568").
    backend: String,
    /// Envoie un en-tête PROXY v2 au serveur (Paper : proxies.proxy-protocol: true).
    #[serde(default)]
    proxy_protocol_out: bool,
    /// Instance AMP (mode AMP). En mode systemd, `instance_name` sert seulement à
    /// retrouver le journal du serveur.
    #[serde(default)]
    instance_id: String,
    instance_name: String,
    /// Mode sans AMP : unité systemd du serveur (ex. "minecraft@PCSI201").
    #[serde(default)]
    systemd_unit: Option<String>,
    /// Journal du serveur, pour savoir quand il est prêt (« Done (… ») en mode systemd.
    /// Par défaut : /home/amp/.ampdata/instances/<instance_name>/Minecraft/logs/latest.log
    #[serde(default)]
    log_file: Option<String>,
    /// Arrêt après N minutes sans joueur (0 = jamais).
    #[serde(default)]
    idle_stop_minutes: u64,
    /// Après un arrêt pour inactivité, éteint aussi le conteneur AMP de l'instance
    /// (économise environ 300 Mo ; le réveil prend alors une dizaine de secondes de plus).
    #[serde(default = "default_true")]
    stop_instance_when_idle: bool,
    /// Délai après l'état « Ready » d'AMP avant de transférer les joueurs.
    #[serde(default = "default_grace")]
    ready_grace_seconds: u64,
    /// Fichier où sont mémorisées les dernières durées de démarrage (secondes).
    startup_history_file: String,
    /// Description affichée dans la liste des serveurs pendant la veille (texte JSON).
    sleeping_motd: serde_json::Value,
    /// Icône 64x64 affichée pendant la veille.
    #[serde(default)]
    icon: Option<String>,
}

fn default_grace() -> u64 {
    1
}

fn default_true() -> bool {
    true
}

/// Paliers (secondes restantes estimées) annoncés dans le chat pendant l'attente.
const MILESTONES: [u64; 7] = [30, 20, 10, 5, 3, 2, 1];

/// Nombre de démarrages pris en compte dans la moyenne.
const HISTORY_LEN: usize = 10;

// ---------------------------------------------------------------- état par serveur

struct Server {
    cfg: ServerCfg,
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
    /// Icône encodée pour la réponse de statut (« data:image/png;base64,... »).
    favicon: Option<String>,
}

impl Server {
    fn touch(&self) {
        *self.last_activity.lock().unwrap() = Instant::now();
    }

    fn request_wake(&self) {
        if !self.wake.swap(true, Ordering::SeqCst) {
            *self.wake_since.lock().unwrap() = Some(Instant::now());
        }
        self.nudge.lock().unwrap().send(()).ok();
        self.touch();
    }
}

struct Global {
    limbo: String,
    protocols: std::sync::RwLock<HashMap<String, ProtocolIds>>,
}

#[derive(Deserialize)]
struct ProtocolsFile {
    #[serde(default)]
    protocols: HashMap<String, ProtocolIds>,
}

/// Table complète : celle de la config principale + celle du fichier complémentaire.
fn load_protocols(inline: &HashMap<String, ProtocolIds>, file: &Option<String>) -> HashMap<String, ProtocolIds> {
    let mut all = inline.clone();
    if let Some(path) = file {
        match std::fs::read_to_string(path).map_err(anyhow::Error::from).and_then(|t| Ok(toml::from_str::<ProtocolsFile>(&t)?)) {
            Ok(f) => all.extend(f.protocols),
            Err(e) => log(format!("{path} illisible : {e:#}")),
        }
    }
    all
}

fn log(msg: impl AsRef<str>) {
    // journald horodate déjà ; on garde des lignes courtes.
    eprintln!("{}", msg.as_ref());
}

/// Dossier où l'état de chaque serveur est publié pour les autres outils
/// (statut Discord, API du panel) : `/run/reveilleur/<nom>.json`.
const STATE_DIR: &str = "/run/reveilleur";

/// Publie l'état d'un serveur (« veille », « demarrage » ou « pret ») si il a changé.
fn publish_state(srv: &Server, last: &mut String, state: &str) {
    let est = srv.estimate.load(Ordering::SeqCst);
    let key = format!("{state}/{est}");
    if *last == key {
        return;
    }
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let json = serde_json::json!({"state": state, "estimate": est, "since": since});
    let path = format!("{STATE_DIR}/{}.json", srv.cfg.name);
    let tmp = format!("{path}.tmp");
    if std::fs::write(&tmp, json.to_string()).and_then(|_| std::fs::rename(&tmp, &path)).is_ok() {
        *last = key;
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

fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 { T[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if c.len() > 2 { T[n as usize & 63] as char } else { '=' });
    }
    out
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

/// Lit une trame complète (longueur VarInt + contenu), sans rien consommer au-delà.
async fn read_frame(s: &mut TcpStream, max: usize) -> Result<Vec<u8>> {
    let mut len_bytes = Vec::new();
    loop {
        let b = s.read_u8().await?;
        len_bytes.push(b);
        if b & 0x80 == 0 {
            break;
        }
        if len_bytes.len() >= 3 {
            bail!("trame trop longue");
        }
    }
    let (len, _) = read_varint(&len_bytes).ok_or_else(|| anyhow!("varint"))?;
    if len < 1 || len as usize > max {
        bail!("taille de trame invalide");
    }
    let mut body = vec![0u8; len as usize];
    s.read_exact(&mut body).await?;
    Ok(body)
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
    let mut first = [0u8; 1];
    s.peek(&mut first).await?;
    if first[0] == 0xFE {
        return Ok(None); // ancien ping (< 1.7)
    }
    let body = read_frame(s, 1024).await?;
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
    let host = String::from_utf8_lossy(body.get(i..i + slen as usize).ok_or_else(|| anyhow!("hôte"))?).to_string();
    i += slen as usize;
    let port = u16::from_be_bytes([
        *body.get(i).ok_or_else(|| anyhow!("port"))?,
        *body.get(i + 1).ok_or_else(|| anyhow!("port"))?,
    ]);
    i += 2;
    let (intent, _) = read_varint(&body[i..]).ok_or_else(|| anyhow!("intention"))?;
    Ok(Some(Handshake { raw: frame(&body), protocol, host, port, intent }))
}

/// Nom d'hôte « propre » (sans marqueurs Forge/Floodgate, sans point final, en minuscules).
fn clean_host(h: &str) -> String {
    h.split('\0').next().unwrap_or("").trim_end_matches('.').to_lowercase()
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

/// Paquet « login_disconnect » (état login, id 0, raison en texte JSON).
fn login_disconnect(text: &str) -> Vec<u8> {
    let mut p = Vec::new();
    write_varint(&mut p, 0);
    write_string(&mut p, &serde_json::json!({"text": text, "color": "gold"}).to_string());
    frame(&p)
}

// ---------------------------------------------------------------- PROXY protocol

const PROXY_V2_SIG: [u8; 12] = [0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A];

/// Lit un en-tête PROXY v1 ou v2 et renvoie l'adresse réelle du client (si fournie).
async fn read_proxy_header(s: &mut TcpStream) -> Result<Option<SocketAddr>> {
    let mut head = [0u8; 16];
    s.read_exact(&mut head).await?;
    if head[..12] == PROXY_V2_SIG {
        let len = u16::from_be_bytes([head[14], head[15]]) as usize;
        let mut body = vec![0u8; len];
        s.read_exact(&mut body).await?;
        let cmd = head[12] & 0x0F; // 0 = LOCAL, 1 = PROXY
        return Ok(match (cmd, head[13] >> 4) {
            (1, 1) if len >= 12 => Some(SocketAddr::new(
                IpAddr::from([body[0], body[1], body[2], body[3]]),
                u16::from_be_bytes([body[8], body[9]]),
            )),
            (1, 2) if len >= 36 => {
                let mut a = [0u8; 16];
                a.copy_from_slice(&body[..16]);
                Some(SocketAddr::new(IpAddr::from(a), u16::from_be_bytes([body[32], body[33]])))
            }
            _ => None,
        });
    }
    if &head[..6] == b"PROXY " {
        // v1 : texte terminé par \r\n (107 octets maximum).
        let mut line = head.to_vec();
        while !line.ends_with(b"\r\n") {
            if line.len() > 107 {
                bail!("en-tête PROXY v1 trop long");
            }
            line.push(s.read_u8().await?);
        }
        let txt = String::from_utf8_lossy(&line);
        let parts: Vec<&str> = txt.trim_end().split(' ').collect();
        if parts.len() == 6 {
            if let (Ok(ip), Ok(port)) = (parts[2].parse::<IpAddr>(), parts[4].parse::<u16>()) {
                return Ok(Some(SocketAddr::new(ip, port)));
            }
        }
        return Ok(None);
    }
    bail!("en-tête PROXY attendu")
}

/// En-tête PROXY v2 annonçant `src` comme adresse du client.
fn proxy_v2_header(src: SocketAddr, dst: SocketAddr) -> Vec<u8> {
    let mut h = PROXY_V2_SIG.to_vec();
    h.push(0x21); // version 2, commande PROXY
    let (src_ip, dst_ip) = (canonical(src.ip()), canonical(dst.ip()));
    match (src_ip, dst_ip) {
        (IpAddr::V4(s), IpAddr::V4(d)) => {
            h.push(0x11); // TCP sur IPv4
            h.extend_from_slice(&12u16.to_be_bytes());
            h.extend_from_slice(&s.octets());
            h.extend_from_slice(&d.octets());
        }
        (s, d) => {
            let v6 = |ip: IpAddr| match ip {
                IpAddr::V4(a) => a.to_ipv6_mapped(),
                IpAddr::V6(a) => a,
            };
            h.push(0x21); // TCP sur IPv6
            h.extend_from_slice(&36u16.to_be_bytes());
            h.extend_from_slice(&v6(s).octets());
            h.extend_from_slice(&v6(d).octets());
        }
    }
    h.extend_from_slice(&src.port().to_be_bytes());
    h.extend_from_slice(&dst.port().to_be_bytes());
    h
}

fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    }
}

// ---------------------------------------------------------------- relais

/// Ouvre la connexion vers le vrai serveur (avec en-tête PROXY si demandé).
async fn connect_backend(srv: &Server, client_addr: SocketAddr) -> Result<TcpStream> {
    let mut up = TcpStream::connect(&srv.cfg.backend)
        .await
        .with_context(|| format!("connexion à {}", srv.cfg.backend))?;
    up.set_nodelay(true).ok();
    if srv.cfg.proxy_protocol_out {
        let dst = up.peer_addr()?;
        up.write_all(&proxy_v2_header(client_addr, dst)).await?;
    }
    Ok(up)
}

async fn pipe(mut client: TcpStream, mut up: TcpStream, first: &[u8]) -> Result<()> {
    up.write_all(first).await?;
    tokio::io::copy_bidirectional(&mut client, &mut up).await.ok();
    Ok(())
}

/// Réponse à la liste des serveurs pendant la veille (statut + ping), sans démarrer le serveur.
async fn sleeping_status(mut client: TcpStream, srv: &Server, hs: &Handshake) -> Result<()> {
    let req = read_frame(&mut client, 16).await?;
    if req.first() != Some(&0) {
        return Ok(());
    }
    let mut status = serde_json::json!({
        "version": {"name": "En veille", "protocol": hs.protocol},
        "players": {"max": 20, "online": 0},
        "description": srv.cfg.sleeping_motd,
    });
    if let Some(f) = &srv.favicon {
        status["favicon"] = serde_json::Value::String(f.clone());
    }
    let mut p = Vec::new();
    write_varint(&mut p, 0);
    write_string(&mut p, &status.to_string());
    client.write_all(&frame(&p)).await?;
    // Ping (id 1 + entier de 8 octets) : on renvoie le même contenu.
    if let Ok(Ok(ping)) = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut client, 16)).await {
        if ping.first() == Some(&1) {
            client.write_all(&frame(&ping)).await?;
        }
    }
    Ok(())
}

#[derive(PartialEq, Clone, Copy)]
enum Phase {
    Login,
    Config,
    Play,
}

/// Fait patienter le joueur dans le limbo, puis lui envoie un paquet Transfer.
async fn limbo_hold(client: TcpStream, hs: Handshake, srv: Arc<Server>, g: &Global, ids: ProtocolIds) -> Result<()> {
    let mut up = TcpStream::connect(&g.limbo).await.context("connexion au limbo")?;
    up.set_nodelay(true).ok();
    up.write_all(&hs.with_intent(2)).await?;

    let (mut cr, mut cw) = client.into_split();
    let (mut lr, mut lw) = up.into_split();
    let c2l = tokio::spawn(async move {
        tokio::io::copy(&mut cr, &mut lw).await.ok();
    });

    let arrived = Instant::now();
    let name = srv.cfg.display_name.clone();
    let mut ready = srv.ready.clone();
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = vec![0u8; 8192];
    let mut phase = Phase::Login;
    let mut announced = false;
    let mut next_milestone = u64::MAX;
    let mut late_notice = false;
    let result = loop {
        if phase == Phase::Play {
            let est = srv.estimate.load(Ordering::SeqCst) as u64;
            if !announced {
                let msg = format!("Démarrage du serveur {name} en cours, environ {est} s. Ne te déconnecte pas !");
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
                log(format!("[{}] transfert vers {host}:{} après {} s d'attente", srv.cfg.name, hs.port, arrived.elapsed().as_secs()));
                tokio::time::sleep(Duration::from_secs(2)).await;
                break Ok(());
            }
            // En attente : un message par palier franchi (30, 20, 10, 5, 3, 2, 1 s).
            let since = srv.wake_since.lock().unwrap().unwrap_or(arrived);
            let remaining = est.saturating_sub(since.elapsed().as_secs());
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
                // nos propres paquets entre deux trames.
                loop {
                    let Some((len, ln)) = read_varint(&buf) else { break };
                    if !(0..=(1 << 21)).contains(&len) {
                        c2l.abort();
                        bail!("trame limbo invalide");
                    }
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

async fn handle(mut client: TcpStream, lst: Arc<(ListenerCfg, Vec<Arc<Server>>)>, g: Arc<Global>) -> Result<()> {
    client.set_nodelay(true).ok();
    let (lcfg, servers) = (&lst.0, &lst.1);
    let mut addr = client.peer_addr()?;
    if lcfg.proxy_protocol_in {
        if let Some(real) = tokio::time::timeout(Duration::from_secs(5), read_proxy_header(&mut client)).await?? {
            addr = real;
        }
    }
    let hs = match tokio::time::timeout(Duration::from_secs(10), read_handshake(&mut client)).await?? {
        Some(h) => h,
        None => return Ok(()), // ancien ping : ignoré
    };
    let host = clean_host(&hs.host);
    let srv = servers
        .iter()
        .find(|s| s.cfg.hostnames.iter().any(|h| h.eq_ignore_ascii_case(&host)))
        .unwrap_or(&servers[0])
        .clone();
    let ready = *srv.ready.borrow();
    let who = canonical(addr.ip());

    if hs.intent == 1 {
        if ready {
            if let Ok(up) = connect_backend(&srv, addr).await {
                return pipe(client, up, &hs.raw).await;
            }
        }
        return sleeping_status(client, &srv, &hs).await;
    }

    if ready {
        if let Ok(up) = connect_backend(&srv, addr).await {
            srv.players.fetch_add(1, Ordering::SeqCst);
            srv.touch();
            log(format!("[{}] {who} -> serveur ({} en ligne)", srv.cfg.name, srv.players.load(Ordering::SeqCst)));
            let r = pipe(client, up, &hs.raw).await;
            srv.players.fetch_sub(1, Ordering::SeqCst);
            srv.touch();
            return r;
        }
    }

    srv.request_wake();
    let est = srv.estimate.load(Ordering::SeqCst);
    let ids = g.protocols.read().unwrap().get(&hs.protocol.to_string()).copied();
    match ids {
        Some(ids) if !lcfg.bedrock => {
            log(format!("[{}] {who} -> limbo (serveur endormi, réveil demandé)", srv.cfg.name));
            limbo_hold(client, hs, srv, &g, ids).await
        }
        _ => {
            // Bedrock (Geyser) ou version inconnue : pas de transfert possible.
            log(format!("[{}] {who} -> réveil demandé, invité à se reconnecter", srv.cfg.name));
            let msg = format!(
                "Le serveur {} démarre (environ {est} s). Reconnecte-toi dans un instant !",
                srv.cfg.display_name
            );
            client.write_all(&login_disconnect(&msg)).await?;
            client.flush().await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(())
        }
    }
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

/// Pilotage d'un serveur : par l'API AMP, ou directement par systemd (sans AMP).
/// Les états suivent la numérotation d'AMP : 0 arrêté, 10 en démarrage, 20 prêt,
/// 40 en arrêt, 100 en échec.
enum Backend {
    Amp { amp: Amp, ads: Amp, instance: String },
    Systemd { unit: String, log_file: String },
}

impl Backend {
    fn state(&mut self) -> Result<i64> {
        match self {
            Backend::Amp { amp, .. } => amp.state(),
            Backend::Systemd { unit, log_file } => {
                let out = std::process::Command::new("systemctl")
                    .args(["show", "-p", "ActiveState", "--value", unit.as_str()])
                    .output()?;
                Ok(match String::from_utf8_lossy(&out.stdout).trim() {
                    "active" => {
                        // Le port s'ouvre avant le chargement du monde : on attend « Done ».
                        let pret = std::fs::read_to_string(log_file.as_str())
                            .is_ok_and(|l| l.contains("]: Done ("));
                        if pret { 20 } else { 10 }
                    }
                    "activating" | "reloading" => 10,
                    "deactivating" => 40,
                    "failed" => 100,
                    _ => 0,
                })
            }
        }
    }

    fn start(&mut self) -> Result<()> {
        match self {
            Backend::Amp { amp, .. } => amp.call("Core/Start").map(|_| ()),
            Backend::Systemd { unit, .. } => {
                // Efface un éventuel état « failed » (sans bruit si l'unité n'était pas chargée).
                let _ = std::process::Command::new("systemctl")
                    .args(["reset-failed", unit.as_str()])
                    .stderr(std::process::Stdio::null())
                    .status();
                systemctl(&["start", "--no-block", unit.as_str()])
            }
        }
    }

    fn stop(&mut self) -> Result<()> {
        match self {
            Backend::Amp { amp, .. } => amp.call("Core/Stop").map(|_| ()),
            Backend::Systemd { unit, .. } => systemctl(&["stop", "--no-block", unit.as_str()]),
        }
    }

    /// Démarre / arrête le conteneur AMP de l'instance (rien à faire en systemd).
    fn start_host(&mut self) -> Result<()> {
        match self {
            Backend::Amp { ads, instance, .. } => ads
                .call_with("ADSModule/StartInstance", serde_json::json!({"InstanceName": instance}))
                .map(|_| ()),
            Backend::Systemd { .. } => Ok(()),
        }
    }

    fn stop_host(&mut self) -> Result<()> {
        match self {
            Backend::Amp { amp, ads, instance } => {
                amp.session = None;
                ads.call_with("ADSModule/StopInstance", serde_json::json!({"InstanceName": instance}))
                    .map(|_| ())
            }
            Backend::Systemd { .. } => Ok(()),
        }
    }

    fn reset_session(&mut self) {
        if let Backend::Amp { amp, .. } = self {
            amp.session = None;
        }
    }

    fn is_amp(&self) -> bool {
        matches!(self, Backend::Amp { .. })
    }
}

fn systemctl(args: &[&str]) -> Result<()> {
    let st = std::process::Command::new("systemctl").args(args).status()?;
    if st.success() {
        Ok(())
    } else {
        bail!("systemctl {} : code {st}", args.join(" "))
    }
}

/// Boucle de supervision d'un serveur : suit l'état AMP, démarre sur demande, arrête si inactif.
fn supervisor(
    srv: Arc<Server>,
    ready_tx: watch::Sender<bool>,
    nudge: std::sync::mpsc::Receiver<()>,
    mut backend: Backend,
) {
    let name = srv.cfg.name.clone();
    let log = |m: String| log(format!("[{name}] {m}"));
    let mut ready_since: Option<Instant> = None;
    let mut last_start_cmd: Option<Instant> = None;
    let mut last_ads_start: Option<Instant> = None;
    let mut last_error_log: Option<Instant> = None;
    // Arrêt pour inactivité en cours : une fois Minecraft arrêté, on éteint le conteneur.
    let mut idle_stopped = false;
    let mut published = String::new();
    loop {
        let waking = srv.wake.load(Ordering::SeqCst);
        if waking {
            idle_stopped = false;
        }
        let res = backend.state();
        let last_amp_state = res.as_ref().ok().copied();
        match res {
            Ok(st) => {
                if idle_stopped && st == 0 {
                    idle_stopped = false;
                    if srv.cfg.stop_instance_when_idle && backend.is_amp() {
                        log("serveur arrêté : extinction du conteneur AMP".to_string());
                        if let Err(e) = backend.stop_host() {
                            log(format!("échec ADSModule/StopInstance : {e:#}"));
                        }
                    }
                }
                // 20 = Ready ; 0 = Stopped ; 100 = Failed
                if st == 20 {
                    let since = *ready_since.get_or_insert_with(Instant::now);
                    let ok = since.elapsed() >= Duration::from_secs(srv.cfg.ready_grace_seconds);
                    ready_tx.send_if_modified(|r| std::mem::replace(r, ok) != ok);
                    if ok && waking {
                        srv.wake.store(false, Ordering::SeqCst);
                        // Mémorise la durée réelle du démarrage pour affiner l'estimation.
                        if let Some(t0) = srv.wake_since.lock().unwrap().take() {
                            let secs = t0.elapsed().as_secs();
                            let path = &srv.cfg.startup_history_file;
                            let mut h = load_history(path);
                            h.push(secs);
                            if h.len() > HISTORY_LEN {
                                h.drain(..h.len() - HISTORY_LEN);
                            }
                            let txt: Vec<String> = h.iter().map(|s| s.to_string()).collect();
                            std::fs::write(path, txt.join(" ") + "\n").ok();
                            srv.estimate.store(average(&h), Ordering::SeqCst);
                            log(format!("serveur prêt en {secs} s (moyenne {} s)", average(&h)));
                        }
                    }
                } else {
                    ready_since = None;
                    ready_tx.send_if_modified(|r| std::mem::replace(r, false));
                }

                let recently = last_start_cmd.is_some_and(|t| t.elapsed() < Duration::from_secs(60));
                if waking && (st == 0 || st == 100) && !recently {
                    log(format!("démarrage du serveur (état {st})"));
                    match backend.start() {
                        Ok(_) => last_start_cmd = Some(Instant::now()),
                        Err(e) => log(format!("échec du démarrage : {e:#}")),
                    }
                }

                let idle = srv.cfg.idle_stop_minutes;
                if idle > 0 && st == 20 && !waking && srv.players.load(Ordering::SeqCst) == 0 {
                    let quiet = srv.last_activity.lock().unwrap().elapsed();
                    if quiet >= Duration::from_secs(idle * 60) {
                        log(format!("aucun joueur depuis {idle} min : arrêt du serveur"));
                        match backend.stop() {
                            Ok(_) => idle_stopped = true,
                            Err(e) => log(format!("échec de l'arrêt : {e:#}")),
                        }
                        srv.touch();
                    }
                }
            }
            Err(e) => {
                ready_since = None;
                ready_tx.send_if_modified(|r| std::mem::replace(r, false));
                backend.reset_session();
                // L'instance AMP elle-même (son conteneur) est arrêtée : on la démarre via ADS.
                let recently = last_ads_start.is_some_and(|t| t.elapsed() < Duration::from_secs(90));
                if waking && !recently && backend.is_amp() {
                    log(format!("instance AMP indisponible ({e:#}) : démarrage via ADS"));
                    if let Err(e) = backend.start_host() {
                        log(format!("échec ADSModule/StartInstance : {e:#}"));
                    }
                    last_ads_start = Some(Instant::now());
                } else if !waking && last_error_log.is_none_or(|t| t.elapsed() > Duration::from_secs(600)) {
                    log(format!("état du serveur illisible : {e:#}"));
                    last_error_log = Some(Instant::now());
                }
            }
        }
        // État publié : prêt, en cours de démarrage (réveil demandé ou démarrage manuel), ou en veille.
        let state = if *ready_tx.borrow() {
            "pret"
        } else if srv.wake.load(Ordering::SeqCst)
            || ready_since.is_some()
            // 5 = PreStart, 7 = Configuring, 10 = Starting, 30 = Restarting
            || matches!(last_amp_state, Some(5 | 7 | 10 | 30))
        {
            "demarrage"
        } else {
            "veille"
        };
        publish_state(&srv, &mut published, state);

        // Au repos on vérifie toutes les 10 s, mais une demande de réveil interrompt l'attente.
        let pause = if srv.wake.load(Ordering::SeqCst) { 1 } else { 10 };
        let _ = nudge.recv_timeout(Duration::from_secs(pause));
        while nudge.try_recv().is_ok() {}
    }
}

// ---------------------------------------------------------------- main

fn main() -> Result<()> {
    let path = std::env::args().nth(1).unwrap_or_else(|| "/etc/reveilleur/reveilleur.toml".into());
    let cfg: Config = toml::from_str(&std::fs::read_to_string(&path).with_context(|| path.clone())?)?;
    // Le mot de passe AMP n'est nécessaire que si au moins un serveur passe par AMP.
    let needs_amp = cfg.servers.iter().any(|s| s.systemd_unit.is_none());
    let password = if needs_amp {
        std::fs::read_to_string(&cfg.amp_password_file)
            .context("mot de passe AMP illisible")?
            .trim()
            .to_string()
    } else {
        String::new()
    };

    let mut by_name: HashMap<String, Arc<Server>> = HashMap::new();
    for s in cfg.servers {
        let (ready_tx, ready_rx) = watch::channel(false);
        let (nudge_tx, nudge_rx) = std::sync::mpsc::channel();
        let favicon = s
            .icon
            .as_ref()
            .and_then(|p| std::fs::read(p).ok())
            .map(|b| format!("data:image/png;base64,{}", base64(&b)));
        let srv = Arc::new(Server {
            estimate: AtomicUsize::new(average(&load_history(&s.startup_history_file))),
            cfg: s,
            ready: ready_rx,
            wake: AtomicBool::new(false),
            wake_since: Mutex::new(None),
            nudge: Mutex::new(nudge_tx),
            players: AtomicUsize::new(0),
            last_activity: Mutex::new(Instant::now()),
            favicon,
        });
        let backend = match &srv.cfg.systemd_unit {
            Some(unit) => {
                let log_file = srv.cfg.log_file.clone().unwrap_or_else(|| {
                    format!("/home/amp/.ampdata/instances/{}/Minecraft/logs/latest.log", srv.cfg.instance_name)
                });
                log(format!("[{}] pilotage par systemd ({unit})", srv.cfg.name));
                Backend::Systemd { unit: unit.clone(), log_file }
            }
            None => Backend::Amp {
                amp: Amp {
                    url: format!("{}ADSModule/Servers/{}/API/", cfg.ads_url, srv.cfg.instance_id),
                    user: cfg.amp_user.clone(),
                    password: password.clone(),
                    session: None,
                },
                ads: Amp {
                    url: cfg.ads_url.clone(),
                    user: cfg.amp_user.clone(),
                    password: password.clone(),
                    session: None,
                },
                instance: srv.cfg.instance_name.clone(),
            },
        };
        let sup = srv.clone();
        std::thread::spawn(move || supervisor(sup, ready_tx, nudge_rx, backend));
        by_name.insert(srv.cfg.name.clone(), srv);
    }

    let mut listeners = Vec::new();
    for l in cfg.listeners {
        let servers = l
            .servers
            .iter()
            .map(|n| by_name.get(n).cloned().ok_or_else(|| anyhow!("serveur inconnu : {n}")))
            .collect::<Result<Vec<_>>>()?;
        if servers.is_empty() {
            bail!("écouteur {} sans serveur", l.addr);
        }
        listeners.push(Arc::new((l, servers)));
    }
    let protocols = load_protocols(&cfg.protocols, &cfg.protocols_file);
    let mut known: Vec<&String> = protocols.keys().collect();
    known.sort();
    log(format!("protocoles connus : {}", known.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")));
    let g = Arc::new(Global { limbo: cfg.limbo, protocols: std::sync::RwLock::new(protocols) });
    let (inline, pfile) = (cfg.protocols, cfg.protocols_file);

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(async move {
        let mut tasks = Vec::new();
        // SIGHUP : relit les numéros de paquets sans couper personne.
        {
            let g = g.clone();
            tasks.push(tokio::spawn(async move {
                let Ok(mut hup) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) else { return };
                while hup.recv().await.is_some() {
                    let fresh = load_protocols(&inline, &pfile);
                    let mut known: Vec<String> = fresh.keys().cloned().collect();
                    known.sort();
                    *g.protocols.write().unwrap() = fresh;
                    log(format!("protocoles rechargés : {}", known.join(", ")));
                }
            }));
        }
        for lst in listeners {
            let listener = TcpListener::bind(&lst.0.addr).await.with_context(|| lst.0.addr.clone())?;
            let names: Vec<&str> = lst.1.iter().map(|s| s.cfg.name.as_str()).collect();
            log(format!("écoute sur {} -> {}", lst.0.addr, names.join(", ")));
            let g = g.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    let Ok((sock, _)) = listener.accept().await else { continue };
                    let (lst, g) = (lst.clone(), g.clone());
                    tokio::spawn(async move {
                        if let Err(e) = handle(sock, lst, g).await {
                            log(format!("connexion : {e:#}"));
                        }
                    });
                }
            }));
        }
        for t in tasks {
            t.await.ok();
        }
        Ok(())
    })
}

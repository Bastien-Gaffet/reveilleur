# reveilleur

Mise en veille des serveurs Minecraft, avec une salle d'attente façon 2b2t.
**Un seul processus et un seul limbo pour tous les serveurs** (aujourd'hui PCSI
en 26.2 et Les coccinelles en 26.3), aiguillés selon le port d'entrée puis
l'adresse demandée.

- **Serveur allumé** : le réveilleur relaie la connexion telle quelle.
- **Serveur endormi** : le joueur arrive dans un **limbo** (le vide de l'End, en
  spectateur, sans autre joueur visible), le serveur démarre, un décompte
  s'affiche dans le chat, puis le joueur est **transféré automatiquement** sur le
  vrai serveur, sans se reconnecter.
- **Plus personne depuis 30 minutes** : le serveur est arrêté.

Coût total : **environ 8 Mo de RAM** (réveilleur environ 3 Mo, PicoLimbo
environ 5 Mo), 0 % de CPU au repos, contre environ 250 Mo pour un proxy Velocity.

## Architecture

```
 joueurs Java :25565 ─┐                         ┌─> PCSI (Paper 26.2)        127.0.0.1:25568  [PROXY v2]
 joueurs Java :25566 ─┤                         │
 port 443 (nginx) ────┼─> reveilleur (Rust) ────┼─> Les coccinelles (26.3)   127.0.0.1:25567
   [PROXY v1]         │   environ 3 Mo          │
 Geyser (Bedrock) ────┘                         └─> PicoLimbo (partagé)      127.0.0.1:30066
   [PROXY v2]                  │
                               └─ API AMP via ADS : état, démarrage, arrêt (un thread par serveur)
```

### Écouteurs (`[[listeners]]`)

| Adresse | Serveurs | Particularité |
|---|---|---|
| `0.0.0.0:25565` | pcsi, coccinelles | port public de PCSI |
| `0.0.0.0:25566` | coccinelles | port public de Les coccinelles |
| `127.0.0.1:25570` | pcsi, coccinelles | trafic Minecraft du multiplexeur nginx du port 443, précédé d'un en-tête PROXY |
| `127.0.0.1:25569` | pcsi | Geyser (Bedrock), en-tête PROXY, `bedrock = true` |

Sur un écouteur, le premier serveur est celui par défaut ; les autres sont
choisis si l'adresse demandée figure dans leurs `hostnames`
(ex. `survie.example.com` par le port 443).

### Vraies adresses IP (PROXY protocol)

- **Entrée** : nginx (`proxy_protocol on`) et Geyser (`advanced.java.use-haproxy-protocol: true`)
  envoient l'IP réelle du joueur ; le réveilleur lit les en-têtes v1 et v2.
- **Sortie** : pour PCSI (`proxy_protocol_out = true`, Paper
  `proxies.proxy-protocol: true`), le réveilleur envoie un en-tête v2 : Paper,
  les bans, CoreProtect et Plan voient la vraie IP au lieu de `127.0.0.1`.
- Côté nginx, le trafic HTTPS passe par une étape interne
  (`listen 127.0.0.1:8444 proxy_protocol`) qui retire l'en-tête avant les vhosts :
  **les sites HTTPS n'ont pas été modifiés**.

### Déroulé d'une connexion

1. Le réveilleur lit **uniquement** la poignée de main (handshake) : version du
   protocole, adresse demandée, intention (liste des serveurs, connexion ou
   transfert).
2. **Ping de la liste des serveurs** : relayé au vrai serveur s'il est prêt,
   sinon le réveilleur répond lui-même avec la description `sleeping_motd` et
   l'icône du serveur concerné (aucun démarrage).
3. **Connexion, serveur prêt** : relais brut dans les deux sens
   (`copy_bidirectional`), sans décodage. Le compteur de joueurs augmente.
4. **Connexion, serveur endormi** :
   - le joueur est relié à PicoLimbo, et la demande de réveil réveille
     immédiatement la boucle de supervision ;
   - le réveilleur suit les trames envoyées au client (sans compression, sans
     chiffrement côté limbo) pour savoir quand le joueur est « en jeu » ;
   - il **insère ses propres paquets** entre deux trames : messages de chat
     (estimation du temps, paliers à 30, 20, 10, 5, 3, 2 et 1 s) ;
   - quand AMP indique que le serveur est prêt, il insère un paquet
     **`transfer`** (natif depuis Minecraft 1.20.5) vers la même adresse. Le
     client se reconnecte tout seul et tombe cette fois sur le vrai serveur.

### État publié pour les autres outils

À chaque changement, le réveilleur écrit `/run/reveilleur/<serveur>.json`
(`{"state": "veille" | "demarrage" | "pret", "estimate": <s>, "since": <horodatage>}`,
dossier créé par systemd via `RuntimeDirectory=reveilleur`). Le statut Discord
(`discord-status`) et l'API du panel (`/api/status`, champ `state`) s'en servent
pour afficher « En veille » au lieu de « Hors ligne ».

### Joueurs Bedrock et versions inconnues

Un client Bedrock (via Geyser) ou un client dont la version n'est pas dans
`[protocols]` (par exemple via ViaVersion) ne peut pas être transféré : le
réveilleur déclenche le réveil et le déconnecte avec « Le serveur X démarre
(environ N s). Reconnecte-toi dans un instant ! ».

### Supervision (un thread par serveur)

- Interroge l'état AMP toutes les 10 s au repos, toutes les secondes pendant un
  réveil.
- Si le conteneur AMP de l'instance est lui-même arrêté, le démarre via ADS
  (`ADSModule/StartInstance`), puis démarre le serveur (`Core/Start`).
- Mesure chaque démarrage réel et garde les 10 derniers
  (`startup_history_file`) : l'estimation annoncée aux joueurs est leur moyenne.
- Arrête le serveur (`Core/Stop`) après `idle_stop_minutes` sans joueur, puis,
  une fois Minecraft arrêté, **éteint aussi le conteneur AMP de l'instance**
  (`ADSModule/StopInstance`, réglage `stop_instance_when_idle`, actif par
  défaut) : environ 260 à 300 Mo libérés par serveur endormi. Le réveil
  rallume le conteneur (une dizaine de secondes de plus).

## Mode systemd (sans AMP)

Pour un serveur qui n'utilise pas AMP (après une migration, voir le dépôt
`migration-serveur`), ajouter dans sa section `[[servers]]` :

```toml
systemd_unit = "minecraft@PCSI201"
# log_file = "..."  (par défaut : /home/amp/.ampdata/instances/<instance_name>/Minecraft/logs/latest.log)
```

Le réveilleur démarre et arrête alors l'unité systemd (`systemctl start/stop
--no-block`) et considère le serveur **prêt quand l'unité est active ET que son
journal contient « Done ( »** : Minecraft ouvre son port avant d'avoir chargé le
monde, un transfert trop tôt échouerait. Les champs AMP (`ads_url`, `amp_user`,
`amp_password_file`, `instance_id`) deviennent inutiles si aucun serveur ne passe
par AMP. Testé sur un banc d'essai (faux serveur sous systemd) : réveil, attente
de « Done », état publié, arrêt après inactivité.

## Configuration

`deploy/reveilleur.toml.example` (sur le VPS : `/etc/reveilleur/reveilleur.toml`).

Global : `limbo`, `ads_url`, `amp_user`, `amp_password_file`, `protocols_file`
(fichier complémentaire relu à chaud sur `SIGHUP`, alimenté par
`reveilleur-maj`), et
`[protocols."<n°>"]` (numéros des paquets `transfer` et `system_chat`).

Par serveur (`[[servers]]`) :

| Clé | Rôle |
|---|---|
| `name`, `display_name` | identifiant et nom affiché aux joueurs |
| `hostnames` | adresses qui désignent ce serveur |
| `backend` | vrai serveur, en local uniquement |
| `proxy_protocol_out` | envoie la vraie IP à Paper |
| `instance_id`, `instance_name` | instance AMP pilotée |
| `idle_stop_minutes` | arrêt après N minutes sans joueur (0 = jamais) |
| `stop_instance_when_idle` | éteint aussi le conteneur AMP après un arrêt pour inactivité (défaut : oui) |
| `ready_grace_seconds` | délai après l'état « prêt » avant le transfert |
| `startup_history_file` | historique des durées de démarrage |
| `sleeping_motd`, `icon` | affichage dans la liste des serveurs pendant la veille |

Par écouteur (`[[listeners]]`) : `addr`, `servers`, `proxy_protocol_in`, `bedrock`.

Côté serveur Paper (réglages AMP, car AMP réécrit `server.properties`) :
`AcceptsTransfers=True`, `ServerIPBinding=127.0.0.1`, port interne changé via
`ADSModule/ApplyInstanceConfiguration` (la modification directe est refusée et
l'opération arrête le conteneur de l'instance).

`deploy/picolimbo/server.toml` : limbo partagé (End, spectateur, liste des
joueurs masquée, compression désactivée, message d'accueil générique).

## Sécurité

- Compte AMP **dédié** `reveilleur`, mot de passe aléatoire dans un fichier
  lisible par root seulement. Rôle « Reveilleur » limité à **une** instance :
  `Instances.<id>.Manage`, `Instances.<id>.Start`, `Instances.<id>.Stop`, et, **posées dans
  l'instance elle-même**, `Core.AppManagement.StartApplication` et
  `StopApplication` (les poser seulement côté ADS ne suffit pas). L'accès aux
  autres instances est refusé.
- Le vrai serveur n'écoute plus qu'en local : impossible de contourner le
  réveilleur.
- Le serveur Paper reste en `online-mode` : l'authentification Mojang est
  inchangée (pas de « forwarding » comme avec Velocity).

## Coût (mesuré)

| Processus | RAM | CPU au repos |
|---|---|---|
| `reveilleur` (tous serveurs confondus) | environ 3 Mo (plafond systemd 32 Mo) | 0 % |
| `pico_limbo` | environ 5 à 9 Mo (plafond 64 Mo) | 0 % |

## Optimisations

- Runtime Tokio mono-thread, binaire Rust de 1 Mo (`opt-level = "s"`, LTO,
  symboles retirés).
- Relais brut sans décodage quand le serveur est prêt.
- Seul ce qui est nécessaire est décodé : poignée de main et identifiants de
  trames côté limbo.
- La boucle de supervision dort entre deux vérifications et se réveille sur
  demande.

## Pièges rencontrés

- **Emojis dans le chat** : les textes en jeu passent en NBT, en UTF-8 modifié
  de Java. Un emoji (hors BMP) envoyé en UTF-8 standard fait planter le client
  (« Failed to decode packet system_chat »). Le réveilleur filtre ces
  caractères ; dans PicoLimbo, ne pas en mettre dans les messages en jeu (la
  liste des serveurs, en JSON, les accepte).
- **Seed par défaut d'AMP** : « CubeCodersPowered », la même pour toutes les
  instances. À changer via `MinecraftModule.Minecraft.WorldSeed`.

## Mise à jour assistée (`deploy/reveilleur-maj`)

Toutes les 5 minutes (`reveilleur-maj.timer`), l'outil :

1. lit la version de Minecraft de chaque serveur (jar en cache) ; si son
   protocole est inconnu, génère les rapports officiels du jar, en extrait les
   numéros de `transfer` et `system_chat`, vérifie que `login_finished` (2) et
   `finish_configuration` (3) n'ont pas changé, dépose une **proposition** et
   prévient sur Discord ;
2. en jeu, PcsiAdmin la présente aux opérateurs avec **[Valider]** / **[Refuser]** ;
3. une fois validée, l'outil l'ajoute à `/etc/reveilleur/protocols.toml` et envoie
   `SIGHUP` au réveilleur, qui **recharge les numéros à chaud** (personne n'est
   coupé), puis met PicoLimbo à jour si sa dernière version annonce la nouvelle
   version de Minecraft (remplacement atomique du binaire).

L'outil est idempotent : une exécution interrompue peut être relancée sans
doublon. Testé de bout en bout le 2026-10-10 : protocole 777 retiré puis
redécouvert, validé et rechargé ; PicoLimbo passé de 1.14.1 à 1.15.0.

## Entretien à chaque nouvelle version de Minecraft

Normalement automatique (voir ci-dessus). À la main si besoin :

1. Récupérer les nouveaux numéros de paquets depuis le jar du serveur :
   ```bash
   java -DbundlerMainClass=net.minecraft.data.Main -jar server.jar --reports
   # generated/reports/packets.json -> play / clientbound / minecraft:transfer et minecraft:system_chat
   ```
2. Ajouter une section `[protocols."<nouveau protocole>"]` dans la config.
3. Mettre à jour PicoLimbo (il suit les nouvelles versions).

Versions actuelles : 26.2 (protocole 776 : transfer 129, system_chat 121) et
26.3 (protocole 777 : transfer 132, system_chat 124). Les identifiants
`login_finished` (2) et `finish_configuration` (3), utilisés pour savoir quand
le joueur est en jeu, sont stables depuis la 1.20.2.

## Compilation et déploiement

```bash
cargo build --release
install -m 755 target/release/reveilleur /usr/local/bin/reveilleur
cp deploy/systemd/*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now picolimbo reveilleur
```

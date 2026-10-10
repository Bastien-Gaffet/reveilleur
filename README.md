# reveilleur

Mise en veille des serveurs Minecraft, avec une salle d'attente façon 2b2t.

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
                      survie.example.com:25566
                                     |
                              +------v------+
                              |  reveilleur |  (Rust, environ 3 Mo)
                              +--+-------+--+
          serveur prêt :         |       |        serveur endormi :
          relais brut            |       |        limbo + réveil
                     +-----------v+     +v-------------------+
                     | Paper       |     | PicoLimbo          |
                     | 127.0.0.1:  |     | 127.0.0.1:30066    |
                     | 25567       |     | (environ 5 Mo)     |
                     +------^------+     +--------------------+
                            |
                     API AMP (via ADS) : démarrage / arrêt / état
```

### Déroulé d'une connexion

1. Le réveilleur lit **uniquement** la poignée de main (handshake) : version du
   protocole, adresse demandée, intention (liste des serveurs, connexion ou
   transfert).
2. **Ping de la liste des serveurs** : relayé au vrai serveur s'il est prêt,
   sinon à PicoLimbo, qui affiche « en veille, connecte-toi pour le réveiller »
   avec l'icône du serveur.
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

### Supervision (thread séparé)

- Interroge l'état AMP toutes les 10 s au repos, toutes les secondes pendant un
  réveil.
- Si le conteneur AMP de l'instance est lui-même arrêté, le démarre via ADS
  (`ADSModule/StartInstance`), puis démarre le serveur (`Core/Start`).
- Mesure chaque démarrage réel et garde les 10 derniers
  (`startup_history_file`) : l'estimation annoncée aux joueurs est leur moyenne.
- Arrête le serveur (`Core/Stop`) après `idle_stop_minutes` sans joueur.

## Configuration

`deploy/coccinelles.toml.example` :

| Clé | Rôle |
|---|---|
| `listen` | adresse publique (ex. `0.0.0.0:25566`) |
| `backend` | vrai serveur, en local uniquement (ex. `127.0.0.1:25567`) |
| `limbo` | PicoLimbo (ex. `127.0.0.1:30066`) |
| `ads_url`, `instance_id`, `instance_name` | instance AMP pilotée |
| `amp_user`, `amp_password_file` | compte AMP dédié (voir Sécurité) |
| `idle_stop_minutes` | arrêt après N minutes sans joueur (0 = jamais) |
| `ready_grace_seconds` | délai après l'état « prêt » avant le transfert |
| `startup_history_file` | historique des durées de démarrage |
| `[protocols."<n°>"]` | numéros des paquets `transfer` et `system_chat` pour chaque version de protocole |

Côté serveur Paper (réglages AMP, car AMP réécrit `server.properties`) :
`AcceptsTransfers=True`, `ServerIPBinding=127.0.0.1`, port interne changé via
`ADSModule/ApplyInstanceConfiguration` (la modification directe est refusée).

`deploy/picolimbo/server.toml` : config du limbo (End, spectateur, liste des
joueurs masquée, compression désactivée, message d'accueil).

## Sécurité

- Compte AMP **dédié** `reveilleur`, mot de passe aléatoire dans un fichier
  lisible par root seulement. Rôle « Reveilleur » limité à **une** instance :
  `Instances.<id>.Manage`, `Instances.<id>.Start`, et, **posées dans
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
| `reveilleur` | environ 3 Mo (plafond systemd 32 Mo) | 0 % |
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

## Entretien à chaque nouvelle version de Minecraft

1. Récupérer les nouveaux numéros de paquets depuis le jar du serveur :
   ```bash
   java -DbundlerMainClass=net.minecraft.data.Main -jar server.jar --reports
   # generated/reports/packets.json -> play / clientbound / minecraft:transfer et minecraft:system_chat
   ```
2. Ajouter une section `[protocols."<nouveau protocole>"]` dans la config.
3. Mettre à jour PicoLimbo (il suit les nouvelles versions).

## Compilation et déploiement

```bash
cargo build --release
install -m 755 target/release/reveilleur /usr/local/bin/reveilleur
cp deploy/systemd/*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now picolimbo-coccinelles reveilleur-coccinelles
```

## Feuille de route

- **Un seul réveilleur et un seul limbo pour plusieurs serveurs**, aiguillés
  selon l'adresse et le port d'entrée (Les coccinelles et PCSI), sans RAM
  supplémentaire.
- **Mise à jour assistée des numéros de paquets** : détection d'une nouvelle
  version, extraction automatique depuis le jar, puis validation par un
  opérateur via une commande en jeu.

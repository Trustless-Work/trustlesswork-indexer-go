# Despliegue del Indexer en Railway — Análisis y plan

> Investigación previa al despliegue. Conectando el repo de GitHub a Railway
> (build automático en cada push a la rama elegida).

---

## TL;DR

El repo ya tiene lo más caro hecho: un `Dockerfile` multi-stage limpio, config 100% por
variables de entorno y un sink RabbitMQ. Railway construye directo desde ese Dockerfile sin
tocar nada.

Pero **hay 3 bloqueantes reales** antes de un despliegue productivo en mainnet:

1. **El estado (cursor + watchlist) vive en un archivo local.** Sin un Railway Volume montado,
   cada redeploy/restart borra el `state.json` → el indexer re-arranca desde el tip del RPC y
   **pierde la watchlist**. Esto es lo más importante.
2. **No hay RabbitMQ.** Hay que provisionar un servicio aparte (template de Railway) y, además,
   **declarar la queue + binding** — algo que en local hace el contenedor `rabbitmq-init` y que
   en Railway no tiene equivalente automático.
3. **Los endpoints de health NO están implementados.** El config los expone, pero no hay
   servidor HTTP en el código Go. Conclusión: el servicio se despliega como **worker** (sin
   dominio público, sin `healthcheckPath`). Si pones un healthcheck, el deploy falla.

Lo demás es configuración de variables y un `railway.json` opcional.

---

## 1. Estado del repo (lo que ya sirve)

| Pieza | Estado | Nota |
|---|---|---|
| `Dockerfile` | ✅ Listo | Multi-stage, `CGO_ENABLED=0`, binario estático, `USER` no-root (uid 10001), `tini` como PID 1 (reaping + SIGTERM). Railway lo detecta y usa por defecto. |
| Config por env | ✅ Listo | `caarlos0/env` con prefijos (`RPC_`, `SINK_`, `STATE_`, `HEALTH_`, etc.). Cero archivos `.env` en el contenedor. |
| Graceful shutdown | ✅ Listo | `signal.NotifyContext` con SIGINT/SIGTERM en `cmd/ingest.go`. Encaja con el ciclo de teardown de Railway (SIGTERM → drain → SIGKILL). |
| `.dockerignore` | ✅ Listo | Excluye `.env`, estado local, `bin/`, docs. Contexto de build chico. |
| Sink RabbitMQ | ✅ Listo | Con publisher confirms. |
| Persistencia de estado | ⚠️ Bloqueante | `FileStore` JSON + flock. Filesystem efímero en Railway. → **necesita Volume**. |
| Servidor de health | ❌ No existe | Ver sección 4. |
| Declaración de queue | ⚠️ Falta en prod | El indexer declara el *exchange* (idempotente) pero **no la queue ni el binding**. |

### Por qué el estado es el problema #1

`internal/state/file_store.go` escribe `state.json` de forma atómica (write a `.tmp` + rename)
y usa un `.lock` sidecar (flock) para garantizar **un solo escritor**. Dos consecuencias para
Railway:

- **Filesystem efímero:** sin Volume, el archivo se va en cada deploy. Para un indexer de mainnet
  esto significa re-escanear desde el tip (perdés ledgers intermedios) y **perder la watchlist
  de escrows** descubierta. Inaceptable en producción.
- **Single-writer:** el flock implica que **no podés escalar horizontalmente** (`numReplicas` > 1
  rompería con dos procesos peleando el lock). Mantené **1 réplica**.

---

## 2. Cómo Railway construye un proyecto Go (decisión de builder)

Railway tiene dos builders:

- **Railpack** (default, sucesor de Nixpacks): detecta `go.mod` + `main()` y compila solo. Sirve
  para apps Go simples sin Dockerfile.
- **Dockerfile**: si hay un `Dockerfile` en la raíz, **Railway lo usa siempre** por encima de
  Railpack.

**Para este proyecto: usar el Dockerfile existente.** Ya resuelve binario estático, usuario
no-root, certificados CA (TLS al RPC de Stellar) y `tini`. No reinventar con Railpack.

No hace falta tocar nada para que lo detecte, pero conviene hacerlo explícito con config-as-code
(sección 6).

---

## 3. RabbitMQ en Railway

### Opción recomendada: servicio RabbitMQ dentro del mismo proyecto

Railway tiene templates de un clic (RabbitMQ 4 single-node + management plugin, con la data
persistida en un volume montado en `/var/lib/rabbitmq`). El flujo:

1. En el proyecto Railway: **+ New → Template → RabbitMQ**.
2. El indexer se conecta por **private networking** (gratis, interno, sin salir a internet) usando
   una *reference variable*:

   ```
   RABBITMQ_URL=amqp://${{RabbitMQ.RABBITMQ_DEFAULT_USER}}:${{RabbitMQ.RABBITMQ_DEFAULT_PASS}}@${{RabbitMQ.RAILWAY_PRIVATE_DOMAIN}}:5672/
   ```

   Clave: en la red privada se usa **el puerto real (5672)**, no hay capa de port-mapping.

### El detalle que se escapa: queue + binding

En local, el contenedor one-shot `rabbitmq-init` declara `exchange → queue → binding`
(`stellar.events` ↔ `indexer.events`, routing key `stellar.#`). El indexer **solo declara el
exchange**; si no existe la queue con su binding, **cada mensaje publicado se descarta en
silencio**.

Railway **no tiene equivalente a `depends_on` ni a init-containers**. Opciones:

- **(A) El consumidor declara su queue** (lo idiomático en AMQP): cuando conectes el
  `wallet-backend` / API NestJS como consumidor, que ese servicio declare la queue + binding al
  arrancar. Es el patrón correcto: el consumidor es dueño de su queue.
- **(B) Declaración manual una vez** vía Management UI de RabbitMQ (exponé un dominio temporal al
  puerto 15672) o `rabbitmqadmin`.
- **(C) Pre-deploy command** en el servicio de RabbitMQ que corra el mismo script de
  `rabbitmq-init`.

Para empezar a probar el publish, (A) o (B). En cuanto enganches el consumidor real, (A) es el
destino.

### Alternativa: CloudAMQP externo

Si preferís RabbitMQ gestionado fuera de Railway, poné el `RABBITMQ_URL` de CloudAMQP. Pierdes la
red privada (tráfico sale a internet) pero ganás backups/HA gestionados. Para este indexer, el
template interno alcanza y es más barato.

---

## 4. Health endpoints: la sorpresa

`.env.example` y `docker-compose.yml` prometen `/healthz`, `/readyz`, `/metrics`, `/status` en
`HEALTH_PORT` (8080). **Pero no hay servidor HTTP en el código.** Verificado:

- `internal/config` parsea y valida `HealthConfig` (`HEALTH_ENABLED`, `HEALTH_PORT`)…
- …pero **ningún archivo Go llama `http.ListenAndServe` / registra `/healthz`**. El único
  `net/http` en uso (`internal/ingest/ingest.go:110`) es el cliente del RPC, no un servidor.

### Implicancias para Railway

- **Railway NO exige escuchar en `$PORT`.** Solo lo exige si querés dominio público o healthchecks.
  Un worker de fondo (sin dominio, sin `healthcheckPath`) corre perfecto. **Desplegá como worker.**
- **No configures `healthcheckPath`** en `railway.json`: Railway haría requests a un endpoint
  inexistente y marcaría el deploy como fallido (service unavailable).
- Ojo con el `PORT`: Railway **inyecta** `PORT`, pero el código lee `HEALTH_PORT` (prefijado), así
  que **no hay choque** — simplemente el `PORT` de Railway se ignora.

### Recomendación (mejora, no bloqueante)

Implementar el servidor de health que el config ya promete. Es poco código y te da:
liveness/readiness para Railway, `/metrics` para observabilidad, y `/status` con la versión.
Cuando exista, debe **bindear a `0.0.0.0`** (no `localhost`) y escuchar en `HEALTH_PORT`; ahí sí
podés añadir `healthcheckPath: /readyz`. Es un buen primer PR de "production-readiness".

---

## 5. Volume para el estado (configuración concreta)

1. En el servicio del indexer: **Settings → Volumes → Add Volume**, mount path `/var/lib/indexer`
   (que es el `WORKDIR` del Dockerfile).
2. Variable: `STATE_PATH=/var/lib/indexer/state.json`.
3. **Permisos:** los volumes de Railway se montan como **root**, pero la imagen corre como
   `indexer` (uid 10001). Sin ajuste, el proceso no puede escribir el volume. Solución oficial de
   Railway:

   ```
   RAILWAY_RUN_UID=0
   ```

   Esto fuerza al contenedor a correr como root, que sí puede escribir el volume montado como root.

   > Alternativa más "limpia" (no-root): cambiar el mount a un subpath y `chown` en runtime vía un
   > entrypoint, pero requiere tocar el Dockerfile. Para empezar, `RAILWAY_RUN_UID=0` es lo
   > pragmático.

4. **El volume NO está montado en build-time** ni en pre-deploy, solo en runtime. El código ya
   crea el directorio con `os.MkdirAll` al arrancar, así que está cubierto.
5. **1 sola réplica** (por el flock single-writer). No actives multi-region/scaling para este
   servicio.

---

## 6. `railway.json` recomendado (config-as-code)

Opcional pero recomendado: versiona la intención del build/deploy en el repo. Railway combina esto
con el dashboard y **el archivo gana**.

```json
{
  "$schema": "https://railway.com/railway.schema.json",
  "build": {
    "builder": "DOCKERFILE"
  },
  "deploy": {
    "restartPolicyType": "ALWAYS",
    "numReplicas": 1
  }
}
```

Notas:
- **Sin `healthcheckPath`** a propósito (no hay endpoint todavía — ver sección 4).
- `restartPolicyType: ALWAYS` → si el proceso muere (p.ej. el RPC se cae y entra a STRICT_MODE),
  Railway lo levanta. Combiná con la lógica de catch-up del propio indexer.
- Si más adelante implementás health: agregá `"healthcheckPath": "/readyz"` y subí
  `healthcheckTimeout` si el primer readiness tarda.

---

## 7. Variables de entorno para mainnet (Variables tab del servicio)

```bash
# --- RPC / Network (MAINNET) ---
RPC_URL=https://mainnet.sorobanrpc.com         # o tu RPC de mainnet preferido
NETWORK_NAME=mainnet
NETWORK_PASSPHRASE=Public Global Stellar Network ; September 2015

# --- Escrow identity ---
ESCROW_APPROVED_WASM_HASHES=38c42f54...,1bbce135...,0136f8fc...,3d924165...

# --- Sink ---
SINK_TYPE=rabbitmq
RABBITMQ_URL=amqp://${{RabbitMQ.RABBITMQ_DEFAULT_USER}}:${{RabbitMQ.RABBITMQ_DEFAULT_PASS}}@${{RabbitMQ.RAILWAY_PRIVATE_DOMAIN}}:5672/
RABBITMQ_EXCHANGE=stellar.events
RABBITMQ_PUBLISHER_CONFIRMS=true

# --- State (Volume) ---
STATE_PATH=/var/lib/indexer/state.json
RAILWAY_RUN_UID=0

# --- Logging / errores ---
LOG_FORMAT=json
LOG_LEVEL=info
STRICT_MODE=true

# --- Arranque ---
# 1ra vez: definí desde qué ledger arrancar (0 = tip actual). Tras el 1er boot,
# manda el state.json del volume.
INDEXER_START_LEDGER=0
HEALTH_ENABLED=false   # mientras no exista el servidor HTTP
```

> Para el `RPC_URL` de mainnet: confirmá el endpoint que vayas a usar (SDF público, Validation
> Cloud, QuickNode, etc.). El RPC público de SDF tiene límites de retención de ledgers — relevante
> para tu requisito de *catch-up* confiable.

---

## 8. Checklist de despliegue

- [ ] Provisionar servicio **RabbitMQ** (template) en el proyecto.
- [ ] Declarar **queue + binding** (consumidor, o manual la primera vez).
- [ ] Crear servicio del **indexer** desde GitHub repo (rama productiva).
- [ ] Confirmar que Railway usa el **Dockerfile** (o añadir `railway.json`).
- [ ] Añadir **Volume** en `/var/lib/indexer` + `STATE_PATH` + `RAILWAY_RUN_UID=0`.
- [ ] Cargar **variables de mainnet** (sección 7) con `RABBITMQ_URL` como reference variable.
- [ ] **NO** generar dominio público ni `healthcheckPath` (es un worker).
- [ ] Mantener **1 réplica** (flock single-writer).
- [ ] Verificar **Deploy Logs**: arranque, conexión al RPC, conexión a RabbitMQ, primer Save de estado.

---

## Backlog de "production-readiness" (post-deploy)

1. **Implementar el servidor de health** (`/healthz`, `/readyz`, `/metrics`, `/status`) que el
   config ya promete. Bindear a `0.0.0.0:HEALTH_PORT`. Habilita healthchecks de Railway y
   observabilidad. Buen primer PR.
2. **Auto-declaración de queue** en el consumidor (`wallet-backend`/API) para eliminar el paso
   manual de RabbitMQ.
3. Evaluar mover el estado a una **DB gestionada** (Postgres de Railway) si en el futuro querés
   multi-réplica — eliminaría la dependencia del Volume + flock.

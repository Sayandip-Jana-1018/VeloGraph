# Deploying VeloGraph live

VeloGraph has two parts:
- **The website** (Next.js, in `web/`): what visitors open.
- **The live backend** (optional): Kafka ×3, the Spark cluster running the scoring job, the GBFS producer, and the
  FastAPI gateway that streams results to the browser.

The website works on its own. Without a gateway every page renders from the recorded exports, and the live pages
show the last recorded snapshot, labelled as such. So there are two levels:

| | What you get | Where | Cost | Time |
|---|---|---|---|---|
| **A. Website only** | every page, from the recorded data | Vercel | free | about 15 min |
| **B. Website + live backend** | the live map updating every 30 s, the live cluster page | Vercel + one cloud VM | free with student credits | about 1–2 h |

For the submission and the viva, A is enough. B is for showing the system live on the internet.

## A. The website on Vercel

Vercel is the company behind Next.js, and its free Hobby plan suits a personal project. Deploy with Vercel's
command-line tool from your laptop, not by connecting the GitHub repository. The website's data (`public/data`,
`public/spark-ui`, `src/generated`) is produced by `pnpm sync` and is not in Git, so a build from GitHub would have
no data. The CLI uploads the `web` folder as it is, data included (`web/.vercelignore` skips only `node_modules`
and `.next`).

1. Create a free account at <https://vercel.com/signup> ("Continue with GitHub").
2. In PowerShell on your laptop:

   ```powershell
   cd C:\Users\Sayan\Desktop\int315\web
   pnpm sync                        # copies the latest exports into the website
   pnpm dlx vercel login            # opens the browser to sign in
   pnpm dlx vercel deploy --prod    # first time: answer the set-up questions (below)
   ```

   On the first deploy, answer the set-up questions like this:
   - *Set up and deploy?* yes;
   - *Which scope?* your account;
   - *Link to existing project?* no;
   - *Project name:* `velograph`;
   - *In which directory is your code located?* `./`.

   Keep the detected settings (Next.js, `pnpm install`, `next build`). Vercel then builds in the cloud and prints
   the address, for example `https://velograph.vercel.app`.
3. Open the address and check `/`, `/portal/lake`, `/portal/models` and `/live`. If a portal fails with "Missing
   ….json", the data was not uploaded: run `pnpm sync` again and redeploy.
4. After a new pipeline run, update the site with `pnpm sync` and then `pnpm dlx vercel deploy --prod`.

Without a backend, `/live` and `/cluster` say "the gateway is not answering: this is the recorded snapshot".
That is the designed offline mode.

## B. The live backend on a cloud VM

### 1. Get a server

Any Linux VM with **4 vCPU, 16 GB RAM and 40 GB disk** will do. The Docker stack alone takes about 7 GB. Free or
almost free options for students:
- **Azure for Students:** US$100 credit with a university email, no card. A `B4ms` VM has 4 vCPU and 16 GB.
- **Oracle Cloud Always Free:** an Ampere A1 VM with 4 cores and 24 GB is free, but needs a card to verify and
  sometimes has no capacity. It is ARM: the images used here have ARM versions, and if one does not start, use an
  x86 VM.
- **GitHub Student Developer Pack:** credits for DigitalOcean and others.

Choose **Ubuntu 24.04 LTS**. In the provider's firewall (network security group), allow inbound **22, 80 and 443
only**. This matters: Docker publishes Kafka (9092–9096), Spark (7077, 8080–8082, 4040) and Kafka UI (8089) on all
interfaces, and Docker's rules bypass Ubuntu's own `ufw`. The provider's firewall is what keeps them private.

### 2. Give it a name (for HTTPS)

Browsers only allow the website (on https) to talk to an https gateway, so the VM needs a domain name. A free one:
1. Sign in at <https://www.duckdns.org>.
2. Create a subdomain such as `velograph-api`, so the VM is `velograph-api.duckdns.org`.
3. Point it at the VM's public IP.

### 3. Install the software

```bash
ssh azureuser@<vm-ip>
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git curl
sudo usermod -aG docker $USER && newgrp docker
curl -LsSf https://astral.sh/uv/install.sh | sh && source ~/.bashrc     # Python 3.11 comes through uv
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update && sudo apt install -y caddy
```

### 4. Copy the project and its live data

Clone the code on the VM:

```bash
git clone https://github.com/Sayandip-Jana-1018/VeloGraph.git ~/velograph
```

The jar and the data the live job needs are not in Git. Copy them from your laptop (PowerShell, in the
repository). They come to about 35 MB:

```powershell
ssh azureuser@<vm-ip> "mkdir -p ~/velograph/data/lake/gold ~/velograph/data/run ~/velograph/spark/target/scala-2.12"
scp spark\target\scala-2.12\velograph-spark.jar azureuser@<vm-ip>:~/velograph/spark/target/scala-2.12/
scp -r data\models data\serving azureuser@<vm-ip>:~/velograph/data/
scp -r data\lake\gold\live_stations data\lake\gold\live_profiles azureuser@<vm-ip>:~/velograph/data/lake/gold/
```

### 5. Start Kafka and Spark

```bash
cd ~/velograph
docker compose -f infra/docker-compose.yml --profile kafka --profile spark up -d
docker ps        # wait until the brokers and the Spark master show "healthy"
```

### 6. Start the producer, the scoring job and the gateway

Run each as a service, so it survives logging out and restarts if it stops. Create
`/etc/systemd/system/velograph-producer.service`:

```ini
[Unit]
Description=VeloGraph GBFS producer
After=docker.service

[Service]
User=azureuser
WorkingDirectory=/home/azureuser/velograph/python
ExecStart=/home/azureuser/.local/bin/uv run python -m velograph.ingest.gbfs_producer
Restart=always

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/velograph-gateway.service` the same way. Put your Vercel address in the origins:

```ini
[Unit]
Description=VeloGraph gateway
After=docker.service

[Service]
User=azureuser
WorkingDirectory=/home/azureuser/velograph/python
Environment=VELOGRAPH_CORS_ORIGINS=https://velograph.vercel.app
ExecStart=/home/azureuser/.local/bin/uv run uvicorn velograph.api.main:app --host 127.0.0.1 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now velograph-producer velograph-gateway
# The scoring job runs on the Spark cluster until a stop file appears (data/run/live-station-scoring.stop):
docker exec -d velograph-spark-master /opt/spark/bin/spark-submit --deploy-mode client \
  --class velograph.streaming.LiveStationScoringJob /opt/velograph/jars/velograph-spark.jar
curl -s http://127.0.0.1:8000/api/health     # after about a minute: kafka ok, spark ok, snapshot fresh
```

### 7. HTTPS in front of the gateway

Replace `/etc/caddy/Caddyfile` with the following (`flush_interval -1` keeps the live stream flowing), then run
`sudo systemctl reload caddy`. Caddy fetches the certificate itself.

```
velograph-api.duckdns.org {
    reverse_proxy 127.0.0.1:8000 {
        flush_interval -1
    }
}
```

Check it: `https://velograph-api.duckdns.org/api/health` should answer in a browser.

### 8. Point the website at the gateway

On the laptop, in `web/`:

```powershell
pnpm dlx vercel env add NEXT_PUBLIC_GATEWAY_URL production    # value: https://velograph-api.duckdns.org
pnpm dlx vercel deploy --prod                                  # NEXT_PUBLIC_ values are fixed at build time
```

`/live` should now say live, update every 30 s, and `/cluster` show the workers and brokers.

## Alternative: everything on the VM

To keep it all on one machine, also host the website on the VM:
1. Install Node 24 and pnpm.
2. Copy `web/public/data`, `web/public/spark-ui` and `web/src/generated` from the laptop.
3. Run `NEXT_PUBLIC_GATEWAY_URL=https://<api-name> pnpm build && pnpm start` as a third service.
4. Add a second Caddy site, `<site-name> { reverse_proxy 127.0.0.1:3000 }`.

## Stopping and costs

- **Stop the live system:**
  - `touch ~/velograph/data/run/live-station-scoring.stop`;
  - `sudo systemctl stop velograph-producer velograph-gateway`;
  - `docker compose -f infra/docker-compose.yml down`.
- **Stop paying:** stop or delete the VM in the provider's portal. A stopped VM costs little; a deleted one
  nothing.
- **The Vercel site stays up for free.** When no backend is reachable it falls back to the recorded data.

## Checklist

- [ ] Vercel site deployed and `/portal/lake` shows data.
- [ ] VM firewall: only 22, 80 and 443 open.
- [ ] `docker ps`: three brokers and the Spark master and workers up.
- [ ] `https://<api-name>/api/health`: kafka and spark ok, snapshot fresh.
- [ ] `NEXT_PUBLIC_GATEWAY_URL` set in Vercel, site redeployed, `/live` shows live data.

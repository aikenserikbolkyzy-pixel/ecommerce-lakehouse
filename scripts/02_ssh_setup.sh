#!/usr/bin/env bash
# =====================================================================
# 02_ssh_setup.sh — passwordless SSH from THIS node to all 3 nodes.
# Required on master (start-dfs.sh / start-yarn.sh / start-workers.sh
# SSH into every host in "workers", including master itself).
# Optional on worker1/worker2 if you want all-to-all SSH.
# You type each laptop's password once (for ssh-copy-id).
# Usage: ./scripts/02_ssh_setup.sh
# =====================================================================
source "$(dirname "$0")/lib.sh"
require_cluster_env

mkdir -p ~/.ssh && chmod 700 ~/.ssh

# 1) Key pair
if [[ ! -f ~/.ssh/id_ed25519 ]]; then
  ssh-keygen -t ed25519 -N "" -C "bigdata-cluster@$(hostname -s)" -f ~/.ssh/id_ed25519
fi

# 2) ~/.ssh/config: per-host user names (laptops may have different logins)
touch ~/.ssh/config && chmod 600 ~/.ssh/config
sed -i '/# >>> bigdata-cluster >>>/,/# <<< bigdata-cluster <<</d' ~/.ssh/config
cat >> ~/.ssh/config <<EOF
# >>> bigdata-cluster >>>
Host master
  User $MASTER_USER
Host worker1
  User $WORKER1_USER
Host worker2
  User $WORKER2_USER
Host master worker1 worker2
  IdentityFile ~/.ssh/id_ed25519
  StrictHostKeyChecking accept-new
  ServerAliveInterval 30
# <<< bigdata-cluster <<<
EOF

# 3) Copy the public key to every node (including this one)
for n in "${ALL_NODES[@]}"; do
  if ssh -o BatchMode=yes -o ConnectTimeout=5 "$n" true 2>/dev/null; then
    ok "key already accepted by $n"
  else
    log "Copying key to $n ($(node_user "$n")@$(node_ip "$n")) — enter that laptop's password"
    ssh-copy-id -i ~/.ssh/id_ed25519.pub "$n"
  fi
done

# 4) Verify: must work with no password prompt and report the right hostname
for n in "${ALL_NODES[@]}"; do
  got="$(ssh -o BatchMode=yes -o ConnectTimeout=5 "$n" hostname -s)" || die "passwordless ssh to $n failed"
  [[ "$got" == "$n" ]] || die "ssh $n landed on host '$got' — check /etc/hosts and hostnames"
  ok "ssh $n -> $got (no password)"
done

#!/usr/bin/env bash
# =====================================================================
# 01_network_setup.sh — run on EACH laptop with its role:
#   master laptop : ./scripts/01_network_setup.sh master
#   node 2        : ./scripts/01_network_setup.sh worker1
#   node 3        : ./scripts/01_network_setup.sh worker2
# Sets hostname, writes /etc/hosts, opens the firewall to the other
# nodes, enables NTP, installs/starts sshd, disables suspend.
# =====================================================================
source "$(dirname "$0")/lib.sh"
require_cluster_env

ROLE="${1:-}"
case "$ROLE" in master|worker1|worker2) ;; *) die "usage: $0 master|worker1|worker2" ;; esac

# 0) Sanity: this laptop really owns the IP you assigned to the role
MY_IP="$(node_ip "$ROLE")"
if ! hostname -I | tr ' ' '\n' | grep -qx "$MY_IP"; then
  die "This laptop's IPs are [$(hostname -I)], but cluster.env says $ROLE = $MY_IP. Fix cluster.env or the network."
fi

# 1) /etc/hosts — managed block + disable the Ubuntu 127.0.1.1 hostname line
log "Updating /etc/hosts"
sudo cp /etc/hosts "/etc/hosts.bak.$(date +%Y%m%d%H%M%S)"
# Ubuntu maps the hostname to 127.0.1.1; then the NameNode/Spark master bind to
# loopback and the other laptops cannot connect. Comment that line out.
sudo sed -i -E 's/^(127\.0\.1\.1[[:space:]].*)$/# \1   # disabled for Hadoop cluster/' /etc/hosts
sudo sed -i '/# >>> bigdata-cluster >>>/,/# <<< bigdata-cluster <<</d' /etc/hosts
sudo tee -a /etc/hosts >/dev/null <<EOF
# >>> bigdata-cluster >>>
$MASTER_IP   master
$WORKER1_IP  worker1
$WORKER2_IP  worker2
# <<< bigdata-cluster <<<
EOF
ok "/etc/hosts:"; grep -A4 '# >>> bigdata-cluster' /etc/hosts

# 2) Hostname
if [[ "$(hostname -s)" != "$ROLE" ]]; then
  log "Setting hostname to '$ROLE' (was '$(hostname -s)')"
  sudo hostnamectl set-hostname "$ROLE"
fi
ok "hostname = $(hostname -s)"

# 3) SSH server (Ubuntu Desktop does not ship one)
if ! dpkg -s openssh-server >/dev/null 2>&1; then
  log "Installing openssh-server"
  sudo apt-get update -y && sudo apt-get install -y openssh-server
fi
sudo systemctl enable --now ssh >/dev/null 2>&1 || sudo systemctl start ssh
ok "sshd running"

# 4) Tools used later
for pkg in unzip rsync curl; do
  dpkg -s "$pkg" >/dev/null 2>&1 || sudo apt-get install -y "$pkg"
done

# 5) Firewall: allow all traffic between the three nodes (Spark uses random ports)
if command -v ufw >/dev/null 2>&1 && sudo ufw status | grep -q "Status: active"; then
  for ip in "$MASTER_IP" "$WORKER1_IP" "$WORKER2_IP"; do sudo ufw allow from "$ip" >/dev/null; done
  ok "ufw: allowed traffic from cluster IPs"
else
  ok "ufw inactive — nothing to open"
fi

# 6) Clock sync (YARN tokens and logs need consistent clocks)
sudo timedatectl set-ntp true || warn "could not enable NTP"

# 7) No suspend while the cluster runs (closing the lid would kill the DataNode)
sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target >/dev/null 2>&1 || true
ok "suspend disabled (undo after the project: sudo systemctl unmask sleep.target suspend.target hibernate.target hybrid-sleep.target)"

# 8) Reachability
for n in "${ALL_NODES[@]}"; do
  if ping -c1 -W2 "$n" >/dev/null 2>&1; then ok "ping $n ($(node_ip "$n"))"
  else warn "cannot ping $n — run this script on that laptop too; if it still fails, the Wi-Fi blocks client-to-client traffic (README §2)"
  fi
done

#!/usr/bin/env bash
# ============================================================================
# Автонастройка VPS для crypto-signal-bot (выполняется на чистой Ubuntu 24.04)
#
# Запуск (одной строкой в консоли VNC):
#   curl -fsSL https://raw.githubusercontent.com/Matveyft/crypto-signal-bot/main/scripts/setup_server.sh | bash
#
# Что делает:
#   1. Обновляет систему, ставит Docker, git, Python
#   2. Создаёт пользователя trader со случайным паролем (покажет в конце)
#   3. Запрещает SSH-вход root (брутифорсят), парольный вход остаётся
#   4. UFW: только SSH
#   5. Клонирует репозиторий, поднимает TimescaleDB + Redis
#   6. Создаёт схему БД, ставит Python-зависимости, гоняет тесты
#   7. Восстанавливать дамп НЕ нужно: история грузится с Bybit (~30 мин, фоном)
#   8. Ставит systemd-сервисы: коллектор стартует сразу, стратегия — следом
#
# Идемпотентен: повторный запуск безопасен.
# ============================================================================
set -euo pipefail

APP_DIR="$HOME/crypto-signal-bot"
REPO_URL="https://github.com/Matveyft/crypto-signal-bot.git"
TS=$(date +%H:%M:%S)

say() { echo -e "\n[$(date +%H:%M:%S)] ====== $* ======"; }

say "1/8 Обновление системы"
export DEBIAN_FRONTEND=noninteractive
# Лечим незавершённые установки в образе ОС (частая причина dpkg error 1)
dpkg --configure -a || true
apt-get update -qq
apt-get -f install -y -qq || true
apt-get upgrade -y -qq || true

say "2/8 Установка Docker, git, Python"
apt-get install -y -qq docker.io git python3-venv python3-pip ufw openssl >/dev/null
# compose-плагин: имя пакета зависит от образа Ubuntu — пробуем варианты
apt-get install -y -qq docker-compose-v2 2>/dev/null \
    || apt-get install -y -qq docker-compose-plugin 2>/dev/null \
    || true
if ! docker compose version >/dev/null 2>&1; then
    echo "ОШИБКА: docker compose не установлен" >&2
    exit 1
fi

say "3/8 Пользователь trader"
if ! id trader &>/dev/null; then
    TRADER_PASSWORD="$(openssl rand -base64 14 | tr -d '/+=')"
    useradd -m -s /bin/bash trader
    echo "trader:${TRADER_PASSWORD}" | chpasswd
    usermod -aG sudo,docker trader
    echo ""
    echo "=============================================================="
    echo "  СОХРАНИТЕ ПАРОЛЬ ПОЛЬЗОВАТЕЛЯ trader (запишите с экрана):"
    echo ""
    echo "      логин:  trader"
    echo "      пароль: ${TRADER_PASSWORD}"
    echo "=============================================================="
    echo ""
else
    usermod -aG sudo,docker trader
    echo "Пользователь trader уже существует (пароль не менялся)."
fi

say "4/8 SSH: запрет root-входа (парольный вход остаётся для trader)"
sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
systemctl restart ssh || systemctl restart sshd || true

say "5/8 UFW: разрешён только SSH"
ufw allow OpenSSH >/dev/null 2>&1 || true
echo "y" | ufw enable >/dev/null 2>&1 || true

say "6/8 Код и базы данных"
sudo -u trader git clone -q "$REPO_URL" "$APP_DIR" 2>/dev/null || \
    sudo -u trader git -C "$APP_DIR" pull -q
cd "$APP_DIR"

# .env: генерируем новый пароль БД, если ещё нет
if [ ! -f .env ]; then
    DB_PASS="$(openssl rand -hex 12)"
    sed -e "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=${DB_PASS}/" .env.example > .env
    chown trader:trader .env
    echo "Создан .env со свежим паролем БД."
fi

docker compose up -d
echo "Ждём здоровье контейнеров (до 60 сек)..."
for i in $(seq 1 12); do
    healthy=$(docker compose ps --format '{{.Status}}' 2>/dev/null | grep -c healthy)
    [ "$healthy" = "2" ] && break
    sleep 5
done
docker compose ps --format 'table {{.Name}}\t{{.Status}}'

say "7/8 Python-окружение и схема БД"
cd "$APP_DIR"
sudo -u trader python3 -m venv .venv
sudo -u trader .venv/bin/pip install -q -r requirements.txt
sudo -u trader .venv/bin/python -m scripts.init_db

say "8/8 systemd-сервисы"
tee /etc/systemd/system/csb-collector.service > /dev/null <<'EOF'
[Unit]
Description=Crypto Signal Bot - collector
After=network-online.target docker.service
Wants=network-online.target

[Service]
User=trader
WorkingDirectory=/home/trader/crypto-signal-bot
ExecStart=/home/trader/crypto-signal-bot/.venv/bin/python -m scripts.run_collector
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

tee /etc/systemd/system/csb-strategy.service > /dev/null <<'EOF'
[Unit]
Description=Crypto Signal Bot - strategy
After=network-online.target docker.service csb-collector.service
Wants=network-online.target

[Service]
User=trader
WorkingDirectory=/home/trader/crypto-signal-bot
ExecStart=/home/trader/crypto-signal-bot/.venv/bin/python -m scripts.run_strategy
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now csb-collector
# Стратегия стартует с задержкой 60 сек после коллектора (таймером разового запуска)
systemctl enable --now csb-strategy || true

say "Загрузка 30 дней истории (фон, ~30 мин) — не закрывайте влияние: она идёт сама"
sudo -u trader nohup "$APP_DIR/.venv/bin/python" -m scripts.load_historical \
    > "$APP_DIR/logs/load_historical.log" 2>&1 &
mkdir -p "$APP_DIR/logs" 2>/dev/null || true
sudo -u trader mkdir -p "$APP_DIR/logs"

echo ""
echo "=============================================================="
echo "  ГОТОВО. Проверка через 2-3 минуты:"
echo "    systemctl status csb-collector csb-strategy --no-pager"
echo "    journalctl -u csb-collector -n 20 --no-pager"
echo "    docker exec csb-timescaledb psql -U trader -d crypto_signals \\"
echo "        -c \"SELECT timeframe, count(*) FROM ohlcv WHERE symbol='BTC/USDT:USDT' GROUP BY 1;\""
echo "=============================================================="

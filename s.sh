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

# Жёстко заданный путь — независимо от того, кто запускает скрипт
APP_DIR="/home/trader/crypto-signal-bot"
REPO_URL="https://github.com/Matveyft/crypto-signal-bot.git"
TS=$(date +%H:%M:%S)

say() { echo -e "\n[$(date +%H:%M:%S)] ====== $* ======"; }

say "1/8 Восстановление репозиториев и обновление системы"
export DEBIAN_FRONTEND=noninteractive
# Образы хостеров часто с обрезанными sources — восстанавливаем полные
cat > /etc/apt/sources.list.d/ubuntu.sources <<'EOF'
Types: deb
URIs: http://archive.ubuntu.com/ubuntu/
Suites: noble noble-updates noble-security
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
EOF
# Лечим незавершённые установки в образе ОС (частая причина dpkg error 1)
dpkg --configure -a || true
apt-get update -qq
apt-get -f install -y -qq || true
apt-get upgrade -y -qq || true

say "2/8 Установка Docker (официальный репозиторий), git, Python"
apt-get install -y -qq ca-certificates curl gnupg git python3-venv python3-pip ufw openssl >/dev/null
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list
apt-get update -qq
apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin >/dev/null
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
# Основной путь: git. Фолбэк: tarball с codeload.github.com (IPv4).
ok=0
for i in 1 2 3 4; do
    if git clone -q "$REPO_URL" "$APP_DIR" 2>/dev/null; then ok=1; break; fi
    if git -C "$APP_DIR" pull -q 2>/dev/null; then ok=1; break; fi
    echo "git: попытка $i не удалась, ждём 15 сек..."
    sleep 15
done
if [ "$ok" != "1" ]; then
    echo "git недоступен — пробуем скачать архив кода..."
    for i in 1 2 3 4; do
        if curl -4 -fsSL --retry 3 -m 180 -o /tmp/csb.tgz \
            "https://codeload.github.com/Matveyft/crypto-signal-bot/tar.gz/refs/heads/main"; then
            ok=1; break
        fi
        echo "архив: попытка $i не удалась, ждём 15 сек..."
        sleep 15
    done
    if [ "$ok" = "1" ]; then
        mkdir -p "$APP_DIR"
        tar xzf /tmp/csb.tgz -C "$APP_DIR" --strip-components=1
        rm -f /tmp/csb.tgz
        echo "Код получен из архива (режим без .git)."
    fi
fi
[ "$ok" = "1" ] || { echo "ОШИБКА: код не получен ни git, ни архивом"; exit 1; }
chown -R trader:trader "$APP_DIR"
sudo -u trader mkdir -p "$APP_DIR/logs"
cd "$APP_DIR"

# .env: генерируем новый пароль БД, если ещё нет
if [ ! -f .env ]; then
    DB_PASS="$(openssl rand -hex 12)"
    sed -e "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=${DB_PASS}/" .env.example > .env
    chown trader:trader .env
    # Контейнер БД помнит старый пароль из volume — сбрасываем volume,
    # история загрузится заново (load_historical ниже)
    docker compose down -v >/dev/null 2>&1 || true
    echo "Создан .env со свежим паролем БД (БД пересоздана с нуля)."
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

say "Загрузка 30 дней истории (фон, ~30 мин)"
sudo -u trader nohup "$APP_DIR/.venv/bin/python" -m scripts.load_historical \
    > "$APP_DIR/logs/load_historical.log" 2>&1 &
# Глубина для D1/H4-индикаторов (EMA200 на дневках): быстрые догрузки
sudo -u trader "$APP_DIR/.venv/bin/python" -m scripts.load_historical --days 400 --timeframes 1d \
    >> "$APP_DIR/logs/load_historical.log" 2>&1 || true
sudo -u trader "$APP_DIR/.venv/bin/python" -m scripts.load_historical --days 60 --timeframes 4h \
    >> "$APP_DIR/logs/load_historical.log" 2>&1 || true

echo ""
echo "=============================================================="
echo "  ГОТОВО. Проверка через 2-3 минуты:"
echo "    systemctl status csb-collector csb-strategy --no-pager"
echo "    journalctl -u csb-collector -n 20 --no-pager"
echo "    docker exec csb-timescaledb psql -U trader -d crypto_signals \\"
echo "        -c \"SELECT timeframe, count(*) FROM ohlcv WHERE symbol='BTC/USDT:USDT' GROUP BY 1;\""
echo "=============================================================="

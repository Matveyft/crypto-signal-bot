#!/usr/bin/env bash
# ============================================================================
# Обновление crypto-signal-bot на VPS (запуск из VNC одной командой):
#   curl -fsSL https://raw.githubusercontent.com/Matveyft/crypto-signal-bot/main/u.sh | bash
#
# Что делает: git pull -> миграции БД -> зависимости -> рестарт сервисов.
# ============================================================================
set -euo pipefail
APP=/home/trader/crypto-signal-bot
TS=$(date +%H:%M:%S)

echo "[$TS] 1/4 Обновление кода из GitHub"
sudo -u trader git -C "$APP" pull -q
cd "$APP"

echo "[$TS] 2/4 Миграции БД"
docker exec csb-timescaledb psql -U trader -d crypto_signals -c \
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS limit_price DOUBLE PRECISION;" -q

echo "[$TS] 3/4 Зависимости (если изменились)"
sudo -u trader "$APP/.venv/bin/pip" install -q -r requirements.txt

echo "[$TS] 4/4 Перезапуск сервисов"
systemctl restart csb-collector csb-strategy
sleep 5
systemctl is-active csb-collector csb-strategy

echo ""
echo "Готово. Проверка: journalctl -u csb-strategy -n 10 --no-pager"

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

echo "[$TS] 1/4 Обновление кода из GitHub (до 5 попыток)"
ok=0
for i in 1 2 3 4 5; do
    if sudo -u trader git -C "$APP" pull -q 2>/dev/null; then ok=1; break; fi
    echo "Попытка $i не удалась, ждём 10 сек..."
    sleep 10
done
[ "$ok" = "1" ] || { echo "ОШИБКА: не удалось обновить код с GitHub"; exit 1; }
cd "$APP"

echo "[$TS] 2/4 Миграции БД"
# Сначала гарантируем схему (идемпотентно), потом миграции
sudo -u trader "$APP/.venv/bin/python" -m scripts.init_db || true
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

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
ok=0
for i in 1 2 3 4; do
    if sudo -u trader git -C "$APP" pull -q 2>/dev/null; then ok=1; break; fi
    echo "git: попытка $i не удалась, ждём 15 сек..."
    sleep 15
done
if [ "$ok" != "1" ]; then
    echo "git недоступен — обновляемся из архива..."
    for i in 1 2 3 4; do
        if curl -4 -fsSL --retry 3 -m 180 -o /tmp/csb.tgz \
            "https://codeload.github.com/Matveyft/crypto-signal-bot/tar.gz/refs/heads/main"; then
            tar xzf /tmp/csb.tgz -C "$APP" --strip-components=1
            rm -f /tmp/csb.tgz
            chown -R trader:trader "$APP"
            ok=1; break
        fi
        sleep 15
    done
fi
[ "$ok" = "1" ] || { echo "ОШИБКА: код не обновлён"; exit 1; }
cd "$APP"

echo "[$TS] 2/4 Миграции БД"
# Сначала гарантируем схему (идемпотентно), потом миграции
sudo -u trader "$APP/.venv/bin/python" -m scripts.init_db || true
docker exec csb-timescaledb psql -U trader -d crypto_signals -c \
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS limit_price DOUBLE PRECISION;" -q
# NOT NULL-колонки без дефолта ломали INSERT позиций (NULL -> constraint)
docker exec csb-timescaledb psql -U trader -d crypto_signals -c \
    "ALTER TABLE positions ALTER COLUMN trailing_active SET DEFAULT false;" -q

echo "[$TS] 3/4 Зависимости (если изменились)"
sudo -u trader "$APP/.venv/bin/pip" install -q -r requirements.txt

echo "[$TS] 4/4 Перезапуск сервисов"
systemctl restart csb-collector csb-strategy
sleep 5
systemctl is-active csb-collector csb-strategy

echo ""
echo "Готово. Проверка: journalctl -u csb-strategy -n 10 --no-pager"

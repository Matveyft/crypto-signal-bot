#!/usr/bin/env bash
# ============================================================================
# Полная статистика crypto-signal-bot на VPS (запуск из VNC одной командой):
#   bash <(curl -fsSL https://raw.githubusercontent.com/Matveyft/crypto-signal-bot/main/stat.sh)
#
# Что делает:
#   1. Здоровье: сервисы, контейнеры, лаг данных, ошибки за 24ч
#   2. Счётчики БД + воронка сигналов по сетапам и по дням
#   3. scripts.analyze_signals --dump  (винрейты, R-профиль, дамп всех сделок)
#   4. scripts.analyze_quality         (реплей по 1m свечам, MFE/MAE)
#   5. Отчёт сохраняется в /tmp/csb_stat.txt и уходит ботом в личный чат
#      (чтобы не копировать длинный текст из VNC-консоли вручную)
# ============================================================================
APP=/home/trader/crypto-signal-bot
REPORT=/tmp/csb_stat.txt
DB="docker exec csb-timescaledb psql -U trader -d crypto_signals -t -A"
SUDO=""
[ "$(id -un)" != "trader" ] && SUDO="sudo -u trader"

{
echo "=================== $(date -u '+%Y-%m-%d %H:%M UTC') ==================="
echo "=================== СЕРВИСЫ / КОНТЕЙНЕРЫ ======"
systemctl is-active csb-collector csb-strategy 2>/dev/null | paste - \
    <(echo -e "csb-collector\ncsb-strategy") | awk '{printf "%-18s %s\n", $2, $1}'
docker ps --format "{{.Names}}: {{.Status}}" 2>/dev/null | grep "^csb-"
echo "ошибок за 24ч: collector=$(journalctl -u csb-collector --since '24 hours ago' --no-pager 2>/dev/null | grep -ciE 'error|traceback' || echo '?') strategy=$(journalctl -u csb-strategy --since '24 hours ago' --no-pager 2>/dev/null | grep -ciE 'error|traceback' || echo '?')"
echo "-- последние строки strategy:"
journalctl -u csb-strategy -n 5 --no-pager 2>/dev/null | tail -5

echo ""
echo "=================== ДАННЫЕ ====================="
echo "-- лаг последней 1m свечи, сек (норма < 120):"
$DB -c "SELECT round(EXTRACT(EPOCH FROM (now()-MAX(timestamp)))) FROM ohlcv WHERE timeframe='1m';"
echo "-- свечи по TF (BTC):"
$DB -c "SELECT timeframe || ': ' || count(*) FROM ohlcv WHERE symbol='BTC/USDT:USDT' GROUP BY timeframe ORDER BY timeframe;"
echo "-- funding-снапшотов: $($DB -c 'SELECT count(*) FROM funding_data;' 2>/dev/null)"
echo "-- signals: $($DB -c 'SELECT count(*) FROM signals;' 2>/dev/null) | positions: $($DB -c 'SELECT count(*) FROM positions;' 2>/dev/null)"

echo ""
echo "=================== ВОРОНКА СИГНАЛОВ ==========="
echo "-- по сетапам:"
$DB -c "SELECT strategy || ': ' || count(*) FROM signals GROUP BY strategy ORDER BY count(*) DESC;"
echo "-- по дням (14 дн):"
$DB -c "SELECT to_char(date_trunc('day', created_at), 'MM-DD') || ': ' || count(*) FROM signals WHERE created_at > now() - interval '14 days' GROUP BY 1 ORDER BY 1;"
echo "-- последние 10 сигналов:"
$DB -c "SELECT id || ' ' || to_char(created_at, 'MM-DD HH24:MI') || ' ' || symbol || ' ' || strategy || ' ' || side || ' conf=' || round(confidence::numeric, 2) FROM signals ORDER BY created_at DESC LIMIT 10;"

echo ""
echo "=================== ОТЧЁТ ПО СДЕЛКАМ ==========="
cd "$APP" && $SUDO "$APP/.venv/bin/python" -m scripts.analyze_signals --dump

echo ""
echo "=================== КАЧЕСТВО (РЕПЛЕЙ 1m) ======"
cd "$APP" && $SUDO "$APP/.venv/bin/python" -m scripts.analyze_quality
} 2>&1 | tee "$REPORT"

# Отправка отчёта ботом в личный чат (куски по 150 строк, лимит 4096 символов)
if [ -f "$APP/.env" ]; then
    TG_TOKEN=$(grep -E '^TELEGRAM_BOT_TOKEN=' "$APP/.env" | cut -d= -f2)
    TG_CHAT=$(grep -E '^TELEGRAM_ADMIN_CHAT_ID=' "$APP/.env" | cut -d= -f2)
    if [ -n "$TG_TOKEN" ] && [ -n "$TG_CHAT" ]; then
        rm -f /tmp/csb_chunk_*
        split -l 150 "$REPORT" /tmp/csb_chunk_
        n=0
        for f in /tmp/csb_chunk_*; do
            n=$((n + 1))
            curl -s --max-time 15 -d chat_id="$TG_CHAT" \
                -d text="📊 Статистика ($n):"$'\n'"$(cat "$f")" \
                "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" > /dev/null
            sleep 1
        done
        rm -f /tmp/csb_chunk_*
        echo ""
        echo "Отчёт отправлен в личный чат ($n сообщений) и сохранён в $REPORT"
    fi
fi

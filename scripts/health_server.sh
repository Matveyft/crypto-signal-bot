#!/usr/bin/env bash
# ============================================================================
# Диагностика crypto-signal-bot на VPS (запуск из VNC одной командой):
#   curl -fsSL https://raw.githubusercontent.com/Matveyft/crypto-signal-bot/main/h.sh | bash
# Показывает: сервисы, лаг данных, счётчики БД, последние SCAN, ошибки.
# ============================================================================
APP=/home/trader/crypto-signal-bot
DB="docker exec csb-timescaledb psql -U trader -d crypto_signals -t -A"

echo "=================== СЕРВИСЫ ==================="
systemctl is-active csb-collector csb-strategy | paste - <(echo -e "csb-collector\ncsb-strategy") | awk '{printf "%-18s %s\n", $2, $1}'

echo ""
echo "=================== КОНТЕЙНЕРЫ =================="
docker compose -f $APP/docker-compose.yml ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || docker ps --format "table {{.Name}}\t{{.Status}}"

echo ""
echo "=================== ДАННЫЕ ====================="
echo "-- Свечи по таймфреймам (BTC):"
$DB -c "SELECT timeframe || ': ' || count(*) FROM ohlcv WHERE symbol='BTC/USDT:USDT' GROUP BY timeframe ORDER BY timeframe;"
echo "-- Лаг последней 1m свечи (сек, норма < 120):"
$DB -c "SELECT round(EXTRACT(EPOCH FROM (now()-MAX(timestamp)))) FROM ohlcv WHERE timeframe='1m';"
echo "-- Funding-снапшотов:"
$DB -c "SELECT count(*) FROM funding_data;"
echo "-- Сигналов / позиций:"
$DB -c "SELECT count(*) FROM signals;"
$DB -c "SELECT count(*) FROM positions;"

echo ""
echo "=================== ПОСЛЕДНИЕ SCAN (стратегия) ="
journalctl -u csb-strategy --no-pager -n 200 2>/dev/null | grep SCAN | tail -10

echo ""
echo "=================== ОШИБКИ ЗА 24Ч ============="
journalctl -u csb-collector --since "24 hours ago" --no-pager 2>/dev/null | grep -ciE "error|traceback" | xargs echo "collector ошибок:"
journalctl -u csb-strategy --since "24 hours ago" --no-pager 2>/dev/null | grep -ciE "error|traceback" | xargs echo "strategy ошибок:"
journalctl -u csb-collector --since "24 hours ago" --no-pager 2>/dev/null | grep -iE "error|crashed" | tail -5

echo ""
echo "=================== ЗАГРУЗКА ИСТОРИИ ==========="
tail -3 $APP/logs/load_historical.log 2>/dev/null || echo "(лог недоступен)"
echo "================================================"

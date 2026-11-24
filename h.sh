#!/usr/bin/env bash
# ============================================================================
# Диагностика crypto-signal-bot на VPS (запуск из VNC одной командой):
#   curl -fsSL https://raw.githubusercontent.com/Matveyft/crypto-signal-bot/main/h.sh | bash
# Показывает: сервисы, лаг данных, счётчики БД, последние SCAN, ошибки,
# состояние Citadel (ленты архива, журнал коллектора, теневые сигналы).
# Под trader просит пароль sudo один раз в начале: он нужен для чтения
# журнала сервисов. Без пароля разделы журнала будут пустыми.
# ============================================================================
APP=/home/trader/crypto-signal-bot
DB="docker exec csb-timescaledb psql -U trader -d crypto_signals -t -A"

# Журнал системных сервисов читает только root (и группы adm, systemd-journal).
# Под trader идём через sudo; без пароля чтение молча не удаётся.
JOURNAL="journalctl"
if [ "$(id -u)" != 0 ]; then
    echo "Для чтения журнала сервисов нужен пароль trader:"
    sudo -v 2>/dev/null || echo "(пароль не введён: разделы журнала будут пустыми)"
    JOURNAL="sudo -n journalctl"
fi

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
$JOURNAL -u csb-strategy --no-pager -n 200 2>/dev/null | grep SCAN | tail -10

echo ""
echo "=================== ОШИБКИ ЗА 24Ч ============="
$JOURNAL -u csb-collector --since "24 hours ago" --no-pager 2>/dev/null | grep -ciE "error|traceback" | xargs echo "collector ошибок:"
$JOURNAL -u csb-strategy --since "24 hours ago" --no-pager 2>/dev/null | grep -ciE "error|traceback" | xargs echo "strategy ошибок:"
$JOURNAL -u csb-collector --since "24 hours ago" --no-pager 2>/dev/null | grep -iE "error|crashed" | tail -5

echo ""
echo "=================== ПОСЛЕДНИЙ ТРЕЙСБЕК (strategy)"
$JOURNAL -u csb-strategy --no-pager 2>/dev/null | grep -B 1 -A 20 -m 1 "Traceback" || echo "(трейсбеков нет)"

echo ""
echo "=================== CITADEL (теневые сигналы) ="
# Статус лент архив пишется в Redis; события — таблица shadow_signals;
# сбои и старты — те же строки коллектора в журнале.
CIT_STATUS=$(docker exec csb-redis redis-cli get citadel:status 2>/dev/null)
if [ -z "$CIT_STATUS" ] || [ "$CIT_STATUS" = "(nil)" ]; then
    echo "-- Citadel: нет ключа citadel:status — коллектор не запущен"
    echo "   (enabled: false в config/citadel.yaml) или молчит дольше 15 минут"
elif command -v python3 >/dev/null 2>&1; then
    printf '%s' "$CIT_STATUS" | python3 -c '
import json, sys, time
try:
    s = json.loads(sys.stdin.read())
except ValueError:
    print("-- citadel:status не разобрался")
    sys.exit(0)
age = max(0, int(time.time() - s.get("t", 0) / 1000))
note = "  <- старше 2 минут: коллектор, похоже, не работает" if age > 120 else ""
print("-- Статус обновлён " + str(age) + " с назад" + note)
for name in ("alerts", "trdr", "marks", "book"):
    f = (s.get("feeds") or {}).get(name) or {}
    ok = f.get("ok")
    state = "ok" if ok else ("СБОЙ" if ok is False else "нет данных")
    print("   {:<7} {:<10} сбоев подряд: {:<3} {}".format(name, state, f.get("fails", "?"), f.get("error") or ""))
def short(syms):
    return ", ".join(x.split("/")[0] for x in syms or []) or "-"
print("   покрыто Citadel: " + short(s.get("covered")))
print("   нет у Citadel:  " + short(s.get("uncovered")))
'
else
    echo "-- citadel:status: $CIT_STATUS"
fi

CIT_LOG=$($JOURNAL -u csb-collector --since "24 hours ago" --no-pager -o cat 2>/dev/null | grep -i citadel)
if [ -n "$CIT_LOG" ]; then
    echo "-- Сбои опроса Citadel за 24 ч: $(printf '%s\n' "$CIT_LOG" | grep -c 'сбой №')"
    echo "-- Последние строки Citadel в журнале:"
    printf '%s\n' "$CIT_LOG" | tail -8
else
    echo "-- Строк Citadel в журнале за 24 ч нет: журнал без пароля не читается,"
    echo "   либо коллектор не запускался"
fi

echo "-- Теневые сигналы в БД (with_levels — свежие, с ценой входа; у алертов"
echo "   за первые сутки там 0 — это норма):"
docker exec csb-timescaledb psql -U trader -d crypto_signals -q -c \
    "SELECT strategy, side, count(*) AS n, count(stop) AS with_levels FROM shadow_signals GROUP BY 1, 2 ORDER BY 1, 2;" \
    -c "SELECT count(*) AS events, count(stop) AS with_levels, to_char(max(detected_at), 'YYYY-MM-DD HH24:MI') AS last_event FROM shadow_signals;" 2>&1

echo ""
echo "=================== ЗАГРУЗКА ИСТОРИИ ==========="
tail -3 $APP/logs/load_historical.log 2>/dev/null || echo "(лог недоступен)"
echo "================================================"

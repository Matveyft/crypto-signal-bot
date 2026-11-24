# Crypto Signal Bot (Bybit USDT Perpetuals)

Система сбора рыночных данных и генерации внутридневных торговых сигналов
для TOP-10 криптовалют на Bybit. Гибридная стратегия: тренд + уровни
поддержки/сопротивления. **Execution layer отсутствует** — бот только
сигналит, ордера не выставляет.

## Архитектура

```
Bybit WS/REST ──► collectors ──► TimescaleDB (история) + Redis (real-time)
                                     │
                                     ▼
                    features (индикаторы, S/R, orderbook, regime)
                                     │
                                     ▼
              strategy (4 сетапа + фильтры + риск-менеджмент)
                                     │
                                     ▼
                  run_strategy (сигналы в БД, трекинг позиций)

Citadel REST ──► CitadelCollector ──► shadow_signals + Redis `citadel:*`
(сторонний терминал: только запись для анализа, без позиций и Telegram)
```

**Сетапы:**
- `trend_pullback` — вход по тренду D1 на откате к поддержке (Fib/EMA50/POC)
- `h4_correction_short` — коррекционный шорт: H4-даунтренд внутри D1-фазы роста
- `mean_reversion` — разворот из RSI/BB-экстремума при негативном funding
- `breakout_retest` — пробой H4-консолидации на объёме + успешный ретест

## Установка

Требования: Python 3.14 (тестировалось на 3.14.4), Docker Desktop.

```powershell
cd C:\projects\trading\crypto-signal-bot
python -m venv .venv
.venv\Scripts\Activate.ps1          # PowerShell (Linux: source .venv/bin/activate)
pip install -r requirements.txt

Copy-Item .env.example .env         # задайте POSTGRES_PASSWORD
docker compose up -d                # TimescaleDB (порт 5433!) + Redis
```

Примечания:
- TimescaleDB доступен на порту **5433**, чтобы не конфликтовать
  с локальным PostgreSQL (5432). Меняется в `docker-compose.yml` + `.env`.
- Если PowerShell блокирует активацию venv:
  `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

## Запуск (по шагам)

```bash
python -m scripts.init_db           # таблицы + hypertables
python -m scripts.load_historical   # 30 дней истории (~10-30 мин)
python -m scripts.run_collector     # WS-поток + funding + агрегатор
python -m scripts.run_strategy      # скан сигналов (второй терминал)
python -m pytest tests -q           # unit-тесты
```

Опции `load_historical`: `--days 30 --timeframes 1h,4h --symbols BTC/USDT:USDT`.

## Конфигурация

| Файл | Назначение |
|---|---|
| `config/symbols.yaml` | Список пар, таймфреймы, глубина истории |
| `config/strategy_params.yaml` | Фильтры, параметры сетапов, риск |
| `config/database.yaml` | Подключение к TimescaleDB/Redis (секреты — в `.env`) |
| `config/citadel.yaml` | Сторонний терминал Citadel: интервалы опроса, виды событий |

## Данные

- **TimescaleDB**: `ohlcv` (hypertable, 7-дневные чанки), `funding_data`,
  `signals`, `positions`, `shadow_signals` (теневые сигналы Citadel)
- **Redis** (TTL): текущие свечи 5 мин, стакан 10 сек, CVD 15 мин, funding 1 час,
  данные Citadel `citadel:*` (стакан 2 мин, каскады/ловушки 6 ч, состояние 15 мин)
- **WS-потоки**: `kline.1m`, `publicTrade` (CVD), `orderbook.50` на каждый символ

## Индикаторы

Реализованы нативно на pandas/numpy (RSI, StochRSI, MACD, BB, ATR, ADX, OBV,
VWAP, EMA 20/50/200): pandas-ta не поддерживается, ta-lib требует компиляции.
Покрыто unit-тестами, поведение соответствует классическим формулам (Wilder).

## Надёжность

- Реконнект WS с exponential backoff (1s → 60s)
- Graceful shutdown по SIGTERM/SIGINT
- Health-check: `TimescaleManager.data_lag_seconds()` — alert, если свеча
  старше 2 минут; `BaseCollector.is_healthy`
- Логи: `logs/strategy.log`, daily rotation, уровень через `LOG_LEVEL`
- Идемпотентная запись (ON CONFLICT DO NOTHING) — рестарты безопасны

## Безопасность

- Только публичные API — ключи не нужны
- Секреты в `.env` (в git не попадает, есть `.env.example`)
- API-ключи никогда не логируются

## Citadel (теневые сигналы)

`CitadelCollector` запускается вместе с `run_collector` и читает открытый архив
стороннего терминала [cryptoangel.trade](https://cryptoangel.trade) (без ключей).
Позиций не создаёт, символы не блокирует, в Telegram ничего не шлёт — только
записывает данные, чтобы измерить их на своей истории.

| Лента архива | Что даёт | Куда пишет |
|---|---|---|
| `/api/alerts` | алерты «Тотал Анализ» (только BTC) | `shadow_signals`, Redis `citadel:alerts` |
| `/api/trdr` | метки TRDR: DIAMOND, Double Diamond, ... (BTC) | `shadow_signals`, Redis `citadel:trdr` |
| `/api/marks` | последний каскад или ловушка по монете | `shadow_signals`, Redis `citadel:mark:{symbol}:{tf}` |
| `/api/ob_now` | агрегированный стакан ~15 бирж: глубина и стены | Redis `citadel:book:{symbol}` |

- **Теневой сигнал** — событие с входом, лимиткой, стопом и целями, посчитанными
  правилами бота на момент события. У замеченного с опозданием события цена
  на момент события неизвестна — оно пишется без уровней и кандидатом не считается.
- Citadel знает не все монеты бота: `citadel:status` показывает `covered` / `uncovered`.
  Чего он не отдаёт, того нет в Redis; остальные данные монеты работают как раньше.
- Отчёт: `python -m scripts.analyze_shadow [--days N] [--onset]` — replay той же
  механики, что в `analyze_quality`, и сравнение события с «входом в случайную минуту».
- Выключить: `enabled: false` в `config/citadel.yaml`.

## Что дальше

- Telegram-уведомления вместо print (`StrategyRunner._notify`)
- Вебхук/dashboard для мониторинга счётчиков из `collector.stats`
- Execution layer (выставление ордеров) поверх таблицы `positions`

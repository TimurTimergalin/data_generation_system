# НЕДЕЛЯ 4: Монетизация и рекламная модель

## Тема
Как зарабатывает игра: IAP, ads, их взаимодействие. Whale-анализ, конверсионная воронка, оптимизация revenue.

## Цели обучения
- Анализировать рекламную монетизацию и её каннибализацию с IAP
- Понимать trade-off: ad revenue ↔ IAP revenue
- Проводить whale analysis и анализ распределения платежей
- Строить конверсионные воронки монетизации
- Оценивать total revenue (IAP + ads) как единую метрику

## Контекст: какие встроенные тесты относятся к теме

| Тест | Что меняет | Trade-off |
|------|-----------|-----------|
| `ad_reward_amount` | Награда за рекламу (25/50/100 gems) | Ad просмотры ↔ IAP каннибализация |


## План блока (3 часа)

### Часть 1: Погружение в тему (30 мин)
- Модели монетизации в F2P — IAP, ads, hybrid
- Реклама в мобильных играх: eCPM, fill rate, ad frequency cap
- Каннибализация: когда щедрая реклама убивает IAP
- Whale-эффект: почему 1% платящих делает 50%+ revenue

### Часть 2: Анализ теста + глубокий аудит монетизации (75 мин)
- Анализ `ad_reward_amount`
- Параллельно: whale analysis, revenue breakdown, конверсионные воронки (часть B)

### Часть 3: Поиск проблемы и дизайн своего теста (45 мин)
- Где проблемы в монетизации? Низкая конверсия? Мало повторных покупок? Дисбаланс IAP vs ads?
- Дизайн теста, конфиг

### Часть 4: Генерация и начало анализа (30 мин)
- Генерация, загрузка, начало анализа

## Задание (что сдавать)

### Deliverable: Глава «Как игра зарабатывает» для итогового отчёта

**Часть A — Анализ встроенного теста:**

Анализ `ad_reward_amount`:

1. **Проверка сплита:** SRM-check
2. **Метрики по группам:**
   - Просмотров рекламы на пользователя
   - Ad revenue (оценка: ~$0.02 за просмотр при eCPM ~$20)
   - IAP revenue per user
   - **Total revenue (IAP + ad estimated)** — ключевая метрика
   - IAP конверсия по группам
3. **Статистика:** CI, p-value
4. **Рекомендация:** щедрая реклама каннибализирует IAP? Что перевешивает — рост ad revenue или падение IAP? Какой вариант максимизирует total revenue?

**Часть B — Аудит монетизации:**

1. **Whale analysis:**
   - Распределение платежей: доля revenue от top 1%, top 5%, top 10% плательщиков
   - Профиль whale: сколько платит, как часто, какие продукты покупает
   - Зависимость от китов: насколько рискованна текущая модель?

2. **Revenue breakdown:**
   - Revenue по продуктам: что продаётся лучше всего?
   - IAP vs estimated ad revenue: какая доля у каждого канала?
   - Динамика revenue по дням: растёт, стабильно, падает?

3. **Конверсионная воронка платежей:**
   - shop_view → iap_initiated → iap_purchase (конверсия на каждом шаге)
   - Где теряем: люди не заходят в шоп? Инициируют, но не завершают?
   - Конверсия в первую vs повторные покупки

**Часть C — Свой тест на монетизацию:**

1. **Проблема:** Что нашли? Низкая конверсия? Мало повторных покупок? Каннибализация?
2. **Гипотеза:** монетизационное изменение и ожидаемый эффект
3. **Дизайн теста:** целевая метрика (revenue, conversion, ARPU), guardrails (retention!)
4. **Конфиг + результаты + рекомендация**

**Доп. задание:**
- Когортный LTV: как монетизация меняется с возрастом когорты? Кривая LTV по дням жизни.
- Price sensitivity: есть ли оптимальная ценовая точка? (Можно использовать данные `starter_pack_price` из недели 2.)

## Подсказки и SQL-примеры

### Total revenue (IAP + Ads) по группам ad_reward_amount

```sql
WITH users AS (
    SELECT DISTINCT
        user_id,
        JSONExtractString(ab_tests, 'ad_reward_amount') AS variant
    FROM game_analytics.events
    WHERE JSONExtractString(ab_tests, 'ad_reward_amount') != ''
),
iap AS (
    SELECT user_id, sum(JSONExtractFloat(event_properties, 'price_usd')) AS iap_revenue
    FROM game_analytics.events WHERE event_name = 'iap_purchase' GROUP BY user_id
),
ads AS (
    SELECT user_id, count() AS ad_views
    FROM game_analytics.events WHERE event_name = 'ad_completed' GROUP BY user_id
)
SELECT
    u.variant,
    count() AS total_users,
    round(sum(coalesce(i.iap_revenue, 0)) / count(), 4) AS iap_arpu,
    round(sum(coalesce(a.ad_views, 0)) / count(), 2) AS avg_ad_views,
    round(sum(coalesce(a.ad_views, 0)) * 0.02 / count(), 4) AS est_ad_arpu,
    round(iap_arpu + est_ad_arpu, 4) AS total_arpu
FROM users u
LEFT JOIN iap i ON u.user_id = i.user_id
LEFT JOIN ads a ON u.user_id = a.user_id
GROUP BY u.variant
ORDER BY u.variant;
```

### Whale analysis

```sql
WITH user_spend AS (
    SELECT
        user_id,
        sum(JSONExtractFloat(event_properties, 'price_usd')) AS total_spent
    FROM game_analytics.events
    WHERE event_name = 'iap_purchase'
    GROUP BY user_id
    ORDER BY total_spent DESC
)
SELECT
    count() AS total_payers,
    sum(total_spent) AS total_revenue,
    sumIf(total_spent, rowNumberInAllBlocks() < count() * 0.01) AS top_1pct_revenue,
    round(top_1pct_revenue / total_revenue * 100, 1) AS top_1pct_share,
    sumIf(total_spent, rowNumberInAllBlocks() < count() * 0.10) AS top_10pct_revenue,
    round(top_10pct_revenue / total_revenue * 100, 1) AS top_10pct_share
FROM user_spend;
```

### Конверсионная воронка платежей

```sql
WITH payment_funnel AS (
    SELECT
        user_id,
        max(event_name = 'shop_view') AS step_shop,
        max(event_name = 'iap_initiated') AS step_initiated,
        max(event_name = 'iap_purchase') AS step_purchase,
        max(event_name = 'iap_failed') AS step_failed
    FROM game_analytics.events
    GROUP BY user_id
)
SELECT
    sum(step_shop) AS shop_viewers,
    sum(step_initiated) AS initiated,
    sum(step_purchase) AS purchased,
    sum(step_failed) AS failed,
    round(initiated / shop_viewers * 100, 2) AS shop_to_init_pct,
    round(purchased / initiated * 100, 2) AS init_to_purchase_pct,
    round(purchased / shop_viewers * 100, 2) AS shop_to_purchase_pct
FROM payment_funnel;
```

### Revenue по продуктам

```sql
SELECT
    JSONExtractString(event_properties, 'product_id') AS product,
    count() AS purchases,
    uniqExact(user_id) AS buyers,
    sum(JSONExtractFloat(event_properties, 'price_usd')) AS revenue,
    round(revenue / purchases, 2) AS avg_price
FROM game_analytics.events
WHERE event_name = 'iap_purchase'
GROUP BY product
ORDER BY revenue DESC;
```

### Примеры гипотез для своего теста (монетизация)

| Проблема | Гипотеза | Что менять |
|----------|----------|-----------|
| Низкая конверсия в первую покупку | Скидка 50% на первую покупку | `conversion_mult`, `iap_conversion_mult` |
| Мало повторных покупок | Бонус за вторую покупку в течение 7 дней | `iap_conversion_mult` |
| Реклама каннибализирует IAP | Лимит рекламы 3/день вместо 5 | `ad_watch_mult`, `iap_conversion_mult` |
| Gems слишком легко копить F2P | Сократить бесплатные gems на 30% | Экономические параметры |

---

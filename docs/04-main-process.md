# 4. Главный процесс: покупка билета

Покупка — это **оркестрированная сага**: Order Service ведёт процесс по шагам и при сбое запускает компенсации (см. [ADR-002](05-adr/ADR-002-saga-orchestration.md)).

| Шаг | Участник | Действие | Компенсация |
|---|---|---|---|
| 1 | Inventory | Захолдить места (AVAILABLE → HELD, TTL 15 мин) | `ReleaseHold` — освободить места |
| 2 | Payment | Создать платёж у провайдера, покупатель платит | `RefundPayment` — вернуть деньги |
| 3 | Inventory | Подтвердить холд (HELD → SOLD) | — (если не вышло, компенсируем шаг 2) |
| 4 | Ticketing | Выпустить билеты | Не компенсируется — **только вперёд**: повторяем, пока не выпустим |
| 5 | Notification | Письмо покупателю | Не компенсируется — повторяем |

Шаг 3 — **поворотная точка** (pivot): до неё сагу можно откатить, после неё — только довести до конца. Места проданы и деньги получены, поэтому сбои в шагах 4–5 лечатся повторами, а не возвратом денег.

## Состояния заказа

```mermaid
stateDiagram-v2
    [*] --> PENDING: POST /orders
    PENDING --> AWAITING_PAYMENT: холд создан, платёж создан
    PENDING --> CANCELLED: места заняты (409)

    AWAITING_PAYMENT --> PAID: PaymentSucceeded
    AWAITING_PAYMENT --> CANCELLED: PaymentFailed / отмена покупателем

    PAID --> CONFIRMED: HoldConfirmed
    PAID --> REFUNDING: HoldConfirmFailed

    CONFIRMED --> COMPLETED: TicketsIssued

    REFUNDING --> REFUNDED: PaymentRefunded

    CANCELLED --> [*]
    COMPLETED --> [*]
    REFUNDED --> [*]
```

Каждый переход — одна локальная транзакция в Order DB: обновить `orders.status` + записать следующую команду в `outbox`. Переход разрешён только из ожидаемого состояния (`UPDATE ... WHERE status = 'PAID'`), поэтому повторное событие не продвинет сагу второй раз.

---

## 4.1. Успешный сценарий

```mermaid
sequenceDiagram
    autonumber
    actor U as Покупатель
    participant GW as API Gateway
    participant ORD as Order
    participant INV as Inventory
    participant PAY as Payment
    participant PSP as Платёжный провайдер
    participant K as Kafka
    participant TKT as Ticketing
    participant NTF as Notification

    rect rgba(59,130,246,0.08)
    Note over U,PSP: Синхронная часть — покупатель ждёт ответ
    U->>GW: POST /orders, Idempotency-Key K1, места A-12-07, A-12-08
    GW->>ORD: POST /orders
    ORD->>ORD: INSERT order PENDING (уникальный K1)
    ORD->>INV: POST /holds (orderId)
    INV->>INV: UPDATE seats SET HELD WHERE status = AVAILABLE<br/>обновлено 2 из 2 строк
    INV-->>ORD: 201 holdId, expiresAt = now + 15 мин, сумма
    ORD->>PAY: POST /payments (orderId, сумма, expiresAt = hold - 3 мин)
    PAY->>PSP: создать платёж, Idempotence-Key = orderId
    PSP-->>PAY: providerPaymentId, confirmationUrl
    PAY-->>ORD: 201 paymentId, confirmationUrl
    ORD->>ORD: status = AWAITING_PAYMENT
    ORD-->>GW: 201 orderId, paymentUrl
    GW-->>U: перенаправление на страницу оплаты
    end

    U->>PSP: вводит данные карты, платит

    rect rgba(16,185,129,0.08)
    Note over PSP,NTF: Асинхронная часть — сага через Kafka
    PSP->>GW: вебхук payment.succeeded
    GW->>PAY: POST /webhooks/psp
    PAY->>PAY: проверка подписи, дедуп по providerEventId
    PAY->>PSP: GET статус платежа (сверка)
    PSP-->>PAY: succeeded
    PAY->>PAY: TX: payment = SUCCEEDED + outbox PaymentSucceeded
    PAY-->>PSP: 200 OK

    PAY-)K: PaymentSucceeded
    K-)ORD: PaymentSucceeded
    ORD->>ORD: TX: inbox + status = PAID + outbox ConfirmHold
    ORD-)K: ConfirmHold
    K-)INV: ConfirmHold
    INV->>INV: TX: inbox + seats HELD → SOLD + outbox HoldConfirmed
    INV-)K: HoldConfirmed
    K-)ORD: HoldConfirmed
    ORD->>ORD: TX: status = CONFIRMED + outbox IssueTickets
    ORD-)K: IssueTickets
    K-)TKT: IssueTickets
    TKT->>TKT: TX: inbox + INSERT 2 tickets с QR + outbox TicketsIssued
    TKT-)K: TicketsIssued
    K-)ORD: TicketsIssued
    ORD->>ORD: TX: status = COMPLETED + outbox OrderCompleted
    ORD-)K: OrderCompleted
    K-)NTF: OrderCompleted
    NTF->>NTF: письмо с билетами (ретраи, если SMTP недоступен)
    end

    U->>GW: GET /orders/{id} (опрос раз в 2 сек)
    GW->>ORD: GET /orders/{id}
    ORD-->>U: COMPLETED
```

**Что здесь важно:**
- Каждый «TX» — локальная транзакция одного сервиса. Изменение данных и запись события в outbox коммитятся вместе, поэтому событие не теряется, даже если сервис упадёт сразу после коммита (ADR-003).
- Payment не верит телу вебхука, а сверяет статус запросом к провайдеру: вебхук можно подделать.
- Срок платёжной сессии на 3 минуты короче холда, чтобы покупатель физически не мог заплатить за уже освобождённые места. Но вебхук всё равно может опоздать — это сценарий сбоя 2.

---

## 4.2. Сбой 1: оплата не прошла (или вебхук потерялся)

**Что ломается.** Банк отклонил карту, или покупатель закрыл страницу оплаты. Места захолжены, деньги не списаны. Хуже вариант: провайдер так и не прислал вебхук, и мы не знаем, чем кончилась оплата.

**Как компенсируем.** Освобождаем места (`ReleaseHold`), заказ → `CANCELLED`. Если вебхука нет — Payment сам опрашивает провайдера (сверка), поэтому окончательный статус всё равно будет получен.

```mermaid
sequenceDiagram
    autonumber
    participant PSP as Платёжный провайдер
    participant PAY as Payment
    participant K as Kafka
    participant ORD as Order
    participant INV as Inventory
    participant NTF as Notification

    alt Провайдер прислал вебхук об отказе
        PSP->>PAY: вебхук payment.canceled (отказ банка)
        PAY->>PSP: GET статус (сверка)
        PSP-->>PAY: canceled
        PAY->>PAY: TX: payment = FAILED + outbox PaymentFailed(DECLINED)
    else Вебхук не пришёл
        Note over PAY: Джоб сверки раз в минуту:<br/>платежи CREATED, у которых истёк срок сессии
        PAY->>PSP: GET статус платежа
        PSP-->>PAY: canceled (сессия истекла)
        PAY->>PAY: TX: payment = FAILED + outbox PaymentFailed(EXPIRED)
    end

    PAY-)K: PaymentFailed
    K-)ORD: PaymentFailed
    ORD->>ORD: TX: status AWAITING_PAYMENT → CANCELLED<br/>+ outbox ReleaseHold
    ORD-)K: ReleaseHold
    K-)INV: ReleaseHold
    alt Холд ещё активен
        INV->>INV: TX: seats HELD → AVAILABLE + outbox HoldReleased
    else Холд уже истёк по таймеру
        INV->>INV: ничего не делаем (no-op), отвечаем HoldReleased
    end
    INV-)K: HoldReleased
    K-)ORD: HoldReleased
    ORD->>ORD: TX: компенсация завершена + outbox OrderCancelled
    ORD-)K: OrderCancelled(PAYMENT_FAILED, refunded = false)
    K-)NTF: OrderCancelled
    NTF->>NTF: письмо «оплата не прошла, места освобождены»
```

**Гарантии:** места вернутся в продажу самое позднее через 15 минут (TTL холда) — даже если Order и Payment оба лежат. `ReleaseHold` идемпотентен: повтор или освобождение уже истёкшего холда ничего не ломает.

---

## 4.3. Сбой 2: деньги списаны, а места уже проданы другому

**Что ломается.** Покупатель заплатил в последнюю секунду, но вебхук задержался у провайдера на 20 минут. За это время холд истёк, места вернулись в продажу и их захолдил другой покупатель. У нас деньги без мест — худший случай.

**Как компенсируем.** Inventory пытается продать места, даже если холд истёк (вдруг они ещё свободны). Если хотя бы одно занято — `HoldConfirmFailed`, и Order запускает компенсацию шага 2: **возврат денег**.

```mermaid
sequenceDiagram
    autonumber
    participant PSP as Платёжный провайдер
    participant PAY as Payment
    participant K as Kafka
    participant ORD as Order
    participant INV as Inventory
    participant NTF as Notification

    Note over INV: T+15 мин: джоб истекает холд<br/>A-12-07, A-12-08 → AVAILABLE
    INV-)K: HoldExpired
    K-)ORD: HoldExpired
    ORD->>ORD: помечает holdExpired = true<br/>заказ НЕ отменяет — ждёт итог оплаты
    Note over INV: T+16 мин: другой покупатель захолдил A-12-07

    PSP->>PAY: T+20 мин: запоздавший вебхук payment.succeeded
    PAY->>PSP: GET статус (сверка)
    PSP-->>PAY: succeeded
    PAY->>PAY: TX: payment = SUCCEEDED + outbox PaymentSucceeded
    PAY-)K: PaymentSucceeded
    K-)ORD: PaymentSucceeded
    ORD->>ORD: TX: status = PAID + outbox ConfirmHold
    ORD-)K: ConfirmHold
    K-)INV: ConfirmHold

    alt Все места всё ещё свободны
        INV->>INV: TX: AVAILABLE → SOLD
        INV-)K: HoldConfirmed
        Note over ORD: дальше обычный успешный путь
    else Хотя бы одно место занято
        INV->>INV: ничего не меняем
        INV-)K: HoldConfirmFailed(SEATS_TAKEN, lost = A-12-07)
        K-)ORD: HoldConfirmFailed
        ORD->>ORD: TX: status = REFUNDING + outbox RefundPayment
        ORD-)K: RefundPayment
        K-)PAY: RefundPayment
        PAY->>PSP: возврат, Idempotence-Key = paymentId
        PSP-->>PAY: refund pending
        PSP->>PAY: вебхук refund.succeeded
        PAY->>PAY: TX: payment = REFUNDED + outbox PaymentRefunded
        PAY-)K: PaymentRefunded
        K-)ORD: PaymentRefunded
        ORD->>ORD: TX: status = REFUNDED + outbox OrderCancelled
        ORD-)K: OrderCancelled(SEATS_LOST, refunded = true)
        K-)NTF: OrderCancelled
        NTF->>NTF: письмо «места не удалось сохранить, деньги вернутся за 1–3 дня»
    end
```

**Гарантии:**
- Место не продано дважды: подтверждение в Inventory — условный `UPDATE` только из `HELD` своего холда или из `AVAILABLE`.
- Деньги не потеряны: из `REFUNDING` заказ выходит только по `PaymentRefunded`. Если провайдер не отвечает — `RefundPayment` ретраится с тем же ключом, второй возврат не создастся.
- Если возврат не прошёл за 24 часа — алерт в поддержку (ручной разбор).

---

## 4.4. Сбой 3: Ticketing упал посередине, сообщения пришли дважды

**Что ломается.** Места проданы, деньги получены, Order отправил `IssueTickets`. Ticketing записал билеты в базу и упал, не успев подтвердить сообщение в Kafka. После рестарта Kafka доставит `IssueTickets` ещё раз. Плюс outbox-relay мог отправить `TicketsIssued` и упасть до отметки «отправлено» — Order получит событие дважды.

**Как компенсируем.** Здесь компенсации нет — мы уже после поворотной точки, отменять покупку нельзя. Вместо этого **forward recovery**: повторяем, а идемпотентность гарантирует, что повтор не создаст дублей.

```mermaid
sequenceDiagram
    autonumber
    participant ORD as Order
    participant K as Kafka
    participant TKT as Ticketing
    participant DB as Ticketing DB
    participant R as Outbox relay (Ticketing)

    ORD-)K: IssueTickets (messageId m1, orderId O1)
    K-)TKT: IssueTickets m1
    TKT->>DB: BEGIN
    TKT->>DB: INSERT inbox(m1)
    TKT->>DB: INSERT tickets(O1, A-12-07), (O1, A-12-08)
    TKT->>DB: INSERT outbox TicketsIssued
    TKT->>DB: COMMIT
    Note over TKT: 💥 процесс упал до commit offset в Kafka

    Note over TKT: Перезапуск через 30 сек
    K-)TKT: IssueTickets m1 (повторная доставка)
    TKT->>DB: INSERT inbox(m1)
    DB-->>TKT: unique violation — уже обработано
    TKT->>K: commit offset, больше ничего не делаем

    R->>DB: SELECT из outbox WHERE sent = false
    R-)K: TicketsIssued (messageId m2)
    Note over R: 💥 relay упал до UPDATE outbox SET sent = true
    R->>DB: после рестарта снова видит m2 как неотправленное
    R-)K: TicketsIssued m2 (дубль)
    R->>DB: UPDATE outbox SET sent = true

    K-)ORD: TicketsIssued m2
    ORD->>ORD: TX: inbox(m2) + CONFIRMED → COMPLETED + outbox OrderCompleted
    K-)ORD: TicketsIssued m2 (дубль)
    ORD->>ORD: inbox(m2) уже есть — пропускаем

    opt Ticketing лежит долго
        Note over ORD: Watchdog саги: заказ в CONFIRMED дольше 5 мин
        ORD->>ORD: алерт дежурному, IssueTickets ждёт в Kafka
        Note over ORD,TKT: Деньги НЕ возвращаем — места проданы,<br/>билеты будут выпущены, как только Ticketing поднимется
    end
```

**Гарантии:**
- Два билета на одно место невозможны: inbox отсекает повтор сообщения, а уникальный индекс `tickets(event_id, seat_id)` — даже если inbox по ошибке пропустит.
- Команда не теряется: Kafka хранит её, пока консьюмер не подтвердит обработку.
- Order не продвинет сагу дважды: inbox + переход только из ожидаемого статуса.

---

## Сводка: что где может сломаться

| Сбой | Где | Реакция | Тип |
|---|---|---|---|
| Место уже занято при холде | Inventory, синхронно | 409 покупателю, заказ не создаётся | отказ |
| Payment/PSP недоступен при создании платежа | шаг 2, синхронно | 503 покупателю, `ReleaseHold`, покупатель повторяет с тем же ключом | компенсация |
| Отказ в оплате / вебхук потерян | шаг 2 | `ReleaseHold`, `CANCELLED` (**сбой 1**) | компенсация |
| Оплата прошла после истечения холда | шаг 3 | `RefundPayment`, `REFUNDED` (**сбой 2**) | компенсация |
| Ticketing / Notification упал, дубли сообщений | шаги 4–5 | ретрай + идемпотентность (**сбой 3**) | forward recovery |
| Order упал посреди саги | любой | состояние в БД, после рестарта читает Kafka с последнего offset | восстановление |
| Kafka недоступна | любой | события копятся в outbox, relay дошлёт | восстановление |

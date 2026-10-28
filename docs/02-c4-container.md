# 2. C4: уровень Container

## Контекст (Level 1) — кратко

```mermaid
C4Context
    title TicketFlow — System Context

    Person(customer, "Покупатель", "Покупает билеты через сайт")
    Person(organizer, "Организатор", "Заводит мероприятия и цены")
    Person(controller, "Контролёр", "Сканирует билеты на входе")

    System(tf, "TicketFlow", "Продажа билетов на мероприятия")

    System_Ext(psp, "Платёжный провайдер", "Приём карт, возвраты")
    System_Ext(mail, "Почтовый провайдер", "Доставка писем")

    Rel(customer, tf, "Выбирает места, платит, получает билеты", "HTTPS")
    Rel(organizer, tf, "Публикует мероприятия", "HTTPS")
    Rel(controller, tf, "Погашает билеты", "HTTPS")
    Rel(tf, psp, "Создаёт платежи и возвраты", "HTTPS/REST")
    Rel(psp, tf, "Вебхуки о статусе платежа", "HTTPS")
    Rel(tf, mail, "Отправляет письма", "SMTP/HTTPS")
```

## Container (Level 2)

```mermaid
C4Container
    title TicketFlow — Container diagram

    Person(customer, "Покупатель")
    Person(controller, "Контролёр")

    System_Ext(psp, "Платёжный провайдер", "ЮKassa / Stripe")
    System_Ext(mail, "Почтовый провайдер", "SMTP / SendGrid")

    System_Boundary(tf, "TicketFlow") {
        Container(web, "Web App", "React SPA", "Афиша, схема зала, оформление заказа")
        Container(gw, "API Gateway", "Nginx / Spring Cloud Gateway", "JWT, rate limiting, маршрутизация")

        Container(catalog, "Catalog Service", "Java/Spring или Go", "Мероприятия, залы, цены")
        ContainerDb(catalogDb, "Catalog DB", "PostgreSQL", "events, venues, seat_maps")
        ContainerDb(cache, "Cache", "Redis", "Кэш афиши и схем залов")

        Container(order, "Order Service", "Java/Spring или Go", "Заказы, оркестратор саги")
        ContainerDb(orderDb, "Order DB", "PostgreSQL", "orders, saga_state, outbox, inbox")

        Container(inventory, "Inventory Service", "Java/Spring или Go", "Места, холды, TTL")
        ContainerDb(inventoryDb, "Inventory DB", "PostgreSQL", "seat_inventory, holds, outbox, inbox")

        Container(payment, "Payment Service", "Java/Spring или Go", "Платежи, вебхуки, возвраты")
        ContainerDb(paymentDb, "Payment DB", "PostgreSQL", "payments, refunds, webhook_log, outbox")

        Container(ticketing, "Ticketing Service", "Java/Spring или Go", "Билеты, QR, проход")
        ContainerDb(ticketDb, "Ticketing DB", "PostgreSQL", "tickets, inbox, outbox")

        Container(notify, "Notification Service", "Java/Spring или Go", "Письма покупателям")
        ContainerDb(notifyDb, "Notification DB", "PostgreSQL", "notifications, templates")

        ContainerQueue(kafka, "Kafka", "Apache Kafka", "Команды и события саги")
    }

    Rel(customer, web, "Использует", "HTTPS")
    Rel(web, gw, "API-вызовы", "HTTPS/JSON")
    Rel(controller, gw, "Сканер билетов", "HTTPS/JSON")

    Rel(gw, catalog, "Афиша", "REST")
    Rel(gw, order, "Заказы", "REST")
    Rel(gw, inventory, "Доступность мест", "REST")
    Rel(gw, ticketing, "Билеты, check-in", "REST")

    Rel(catalog, catalogDb, "SQL")
    Rel(catalog, cache, "Кэш")
    Rel(order, orderDb, "SQL")
    Rel(inventory, inventoryDb, "SQL")
    Rel(payment, paymentDb, "SQL")
    Rel(ticketing, ticketDb, "SQL")
    Rel(notify, notifyDb, "SQL")

    Rel(order, inventory, "Создать холд", "REST, sync")
    Rel(order, payment, "Создать платёж", "REST, sync")

    Rel(catalog, kafka, "catalog.events")
    Rel(order, kafka, "Команды + order.events")
    Rel(inventory, kafka, "inventory.events / читает команды")
    Rel(payment, kafka, "payment.events / читает команды")
    Rel(ticketing, kafka, "ticketing.events / читает команды")
    Rel(notify, kafka, "Подписан на события")

    Rel(payment, psp, "Платежи, возвраты", "HTTPS")
    Rel(psp, gw, "Вебхуки", "HTTPS")
    Rel(notify, mail, "Письма", "SMTP")

    UpdateLayoutConfig($c4ShapeInRow="4", $c4BoundaryInRow="1")
```

> Вебхуки провайдера приходят через Gateway, который проксирует их на `POST /webhooks/psp` в Payment Service. Gateway пропускает этот путь без JWT, проверку подписи делает Payment.

## Контейнеры

| Контейнер | Технология | Масштабирование | Комментарий |
|---|---|---|---|
| API Gateway | Nginx / Spring Cloud Gateway | горизонтально | Rate limit на `POST /orders` при старте продаж |
| Catalog | сервис + PostgreSQL + Redis | горизонтально, read-heavy | Афишу можно отдавать из кэша |
| Order | сервис + PostgreSQL | горизонтально | Состояние саги в БД, сервис stateless |
| Inventory | сервис + PostgreSQL | горизонтально, партиции по `event_id` | Горячая точка при старте продаж |
| Payment | сервис + PostgreSQL | 2+ реплики | Изолирован сетевыми политиками |
| Ticketing | сервис + PostgreSQL | горизонтально | Пик за час до начала мероприятия |
| Notification | сервис + PostgreSQL | 1–2 реплики | Не критичен по времени |
| Kafka | 3 брокера, `replication.factor=3`, `min.insync.replicas=2` | — | Ключ сообщения = `orderId` |

## Топики Kafka

| Топик | Кто пишет | Кто читает |
|---|---|---|
| `catalog.events` | Catalog | Inventory |
| `inventory.commands` | Order | Inventory |
| `inventory.events` | Inventory | Order |
| `payment.commands` | Order | Payment |
| `payment.events` | Payment | Order |
| `ticketing.commands` | Order | Ticketing |
| `ticketing.events` | Ticketing | Order, Notification |
| `order.events` | Order | Notification |

Подробная схема сообщений — в [AsyncAPI](03-contracts/asyncapi/ticketflow-events.yaml).

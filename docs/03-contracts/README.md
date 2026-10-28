# 3. Контракты

## Синхронные API (OpenAPI 3.0)

| Файл | Сервис | Кто вызывает |
|---|---|---|
| [catalog-service.yaml](openapi/catalog-service.yaml) | Catalog | Web App, организатор |
| [order-service.yaml](openapi/order-service.yaml) | Order | Web App |
| [inventory-service.yaml](openapi/inventory-service.yaml) | Inventory | Web App (доступность), Order (холды) |
| [payment-service.yaml](openapi/payment-service.yaml) | Payment | Order (создать платёж), провайдер (вебхук) |
| [ticketing-service.yaml](openapi/ticketing-service.yaml) | Ticketing | Web App, сканер контролёра |

Notification Service синхронного API не имеет — он только слушает события.

## Асинхронные сообщения (AsyncAPI 3.0)

[ticketflow-events.yaml](asyncapi/ticketflow-events.yaml) — все топики, команды и события.

## Почему что-то синхронно, а что-то нет

Синхронно — только то, где покупатель **ждёт ответ прямо сейчас**:
- холд мест: покупатель должен сразу узнать, что место заняли;
- создание платежа: нужна ссылка на оплату, чтобы перенаправить покупателя.

Всё после оплаты асинхронно: подтверждение мест, выпуск билетов, письма. Эти шаги нельзя потерять, но можно сделать через секунду, и они должны переживать падение участника (Kafka хранит команду, пока сервис не поднимется). Подробнее — [ADR-002](../05-adr/ADR-002-saga-orchestration.md).

## Правила идемпотентности

| Где | Ключ | Механизм |
|---|---|---|
| `POST /orders` | заголовок `Idempotency-Key` | уникальный индекс в `orders.idempotency_key` |
| `POST /holds` | `orderId` | уникальный индекс `holds.order_id` |
| `POST /payments` | `orderId` | уникальный индекс `payments.order_id`, тот же ключ уходит провайдеру |
| Вебхук провайдера | `providerEventId` | уникальный индекс в `provider_webhook_log` |
| Любое сообщение Kafka | `messageId` | таблица `inbox` у консьюмера |
| `IssueTickets` | `orderId` + `(event_id, seat_id)` | уникальные индексы в `tickets` |
| `RefundPayment` | `paymentId` | один возврат на платёж, ключ уходит провайдеру |

## Как смотреть

- OpenAPI: открыть файл в [Swagger Editor](https://editor.swagger.io) или `npx @redocly/cli preview-docs <file>`.
- AsyncAPI: открыть в [AsyncAPI Studio](https://studio.asyncapi.com).
- Проверка: `npx @redocly/cli lint openapi/*.yaml` и `npx @asyncapi/cli validate asyncapi/ticketflow-events.yaml`.

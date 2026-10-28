# 5. Architecture Decision Records

| № | Решение | Статус |
|---|---|---|
| [ADR-001](ADR-001-service-boundaries.md) | Шесть сервисов, у каждого своя база | Принято |
| [ADR-002](ADR-002-saga-orchestration.md) | Сага покупки с оркестратором в Order Service | Принято |
| [ADR-003](ADR-003-kafka-outbox-inbox.md) | Kafka + Transactional Outbox + Inbox | Принято |
| [ADR-004](ADR-004-seat-holds.md) | Холд мест с TTL через условный UPDATE в PostgreSQL | Принято |
| [ADR-005](ADR-005-idempotency.md) | Идемпотентность на каждой границе | Принято |

Шаблон: **Контекст → Варианты → Решение → Последствия**. ADR не переписываются задним числом: если решение меняется, пишется новый ADR со статусом «Заменяет ADR-00X».

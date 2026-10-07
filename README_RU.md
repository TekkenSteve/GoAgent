# GoAgent

> **GoAgent** — open-source **AgentOS**: слой исполнения агентов для продуктов со встроенным AI. Команда продукта делает продукт — домен, UX, цикл; GoAgent исполняет его агентную половину: долговременные run и plan, инструменты и MCP, стриминг, артефакты, аудит, биллинг и мультитенантность за одним control plane.

[![Web Framework](https://img.shields.io/badge/Fiber-Web%20Framework-blue)](https://github.com/gofiber/fiber)
[![Workflow Engine](https://img.shields.io/badge/Temporal-Workflow%20Engine-blue)](https://temporal.io/)
[![Event Backbone](https://img.shields.io/badge/NATS%20JetStream-Event%20Backbone-blue)](https://nats.io/)
[![SQL Compiler](https://img.shields.io/badge/sqlc-Type--Safe%20SQL-blue)](https://sqlc.dev/)
[![Database Migrations](https://img.shields.io/badge/golang--migrate-Schema%20Updates-blue)](https://github.com/golang-migrate/migrate)
[![Logging](https://img.shields.io/badge/ZeroLog-Structured%20Logging-blue)](https://github.com/rs/zerolog)
[![Metrics](https://img.shields.io/badge/Prometheus-Metrics%20Integration-blue)](https://github.com/ansrivas/fiberprometheus)

## Проблема

Модель — один из слоёв продукта, а не сам продукт. Апгрейды модели бесплатно усиливают этот слой — и никак не затрагивают всё вокруг него. Продукт, чей ключевой цикл включает долгую AI-работу, всё равно обязан инженерно ответить: что происходит, когда процесс умирает посреди run, как трёхдневный диалог переживает рестарты, кто одобрил тот вызов инструмента, что именно агент сделал с моими данными и сколько это стоило.

Каждая AI-команда вручную собирает одну и ту же неблестящую машинерию ради этих ответов — жизненный цикл run, устойчивые к сбоям очереди задач, диспетчеризацию инструментов, стриминг прогресса, хранилище артефактов, аудит, учёт использования. Эта машинерия ничего не знает о продукте: студия флеш-карточек и платформа security-ops нуждаются в *одной и той же* половине — различаются только их существительные.

Пять проблем распределённых систем, каждая усилена недетерминированными рассуждениями:

| Проблема продакшена | Что добавляют агенты | Что обязана взять на себя система |
|---|---|---|
| Долгая работа | диалоги, согласования и циклы наблюдения длятся днями | долговременные ожидания, таймеры, восстановление после сбоев |
| Внешние побочные эффекты | модель *точно* вызовет ваши платёжные и нотификационные инструменты | идемпотентность, повторы, компенсации, шлюзы согласований |
| Недетерминированные решения | одна цель — множество путей рассуждения | шлюзы политик, бюджеты, зафиксированные обоснования |
| Много участников | команды, агенты и люди одновременно | контракты, изоляция, авторизация |
| Постоянные изменения | дрейфуют промпты, схемы инструментов и политики | версионируемые схемы, управляемые миграции |

GoAgent — эта машинерия, превращённая в продукт. LLM отвечает за неопределённые когнитивные шаги; долговременный workflow — за определённый вокруг них жизненный цикл и governance.

## Что такое GoAgent

```mermaid
graph TB
    subgraph PRODUCTS["Ваши продукты — любой домен"]
        direction LR
        P1["студия флеш-карточек<br/>(крафт в диалоге)"]
        P2["платформа security-ops"]
        P3["dev-инструмент · игра · что угодно<br/>с долгой AI-работой"]
    end

    subgraph AGENTOS["GoAgent / AgentOS"]
        CP["Agent Control Plane<br/>runs · plans · signals · capabilities"]
        PP["Durable Process Platform<br/>resources · ledger · governed actions · worksets"]
        NX["Nexus API<br/>долговременные межсервисные операции"]
    end

    subgraph STATE["Долговременность и факты"]
        T["Temporal<br/>состояние · таймеры · повторы · восстановление"]
        N["NATS JetStream<br/>транзакционный outbox → проекции · зеркала"]
        P[("Postgres<br/>ledger · хешированная аудит-цепочка · проекции")]
    end

    subgraph BACKENDS["Агентные бэкенды"]
        NA["нативный GoAgent ReAct<br/>+ MCP-инструменты"]
        LG["LangGraph · runtime<br/>в стиле OpenCode"]
        EXT["HTTP · gRPC ·<br/>внешний Temporal"]
    end

    PRODUCTS -->|"нейтральные к движку контракты<br/>agentos/core · control · process"| AGENTOS
    CP --> BACKENDS
    PP --> STATE
    NX --> T
```

Три решения несут всю конструкцию:

- **Ваши существительные остаются вашими.** Доменная работа приходит как обобщённые `ResourceRef` — набор карточек и кейс безопасности для AgentOS одна и та же сущность. Он никогда не учит вашу доменную модель, а ваш продукт никогда не трогает его внутренности: публичные контракты (`agentos/core`, `agentos/control`, `agentos/process`) не импортируют ни один движок исполнения, что зафиксировано `make check-import-boundary`.
- **Control plane владеет оркестрацией; бэкенды — исполнением.** GoAgent запускает, сигнализирует, контролирует и наблюдает run'ы, *принадлежащие бэкендам*, и компонует их в долговременные планы между бэкендами. Бэкенд — это нативный ReAct-цикл, сервис LangGraph, OpenCode-подобный runtime или любой HTTP/gRPC/Temporal-сервис; граф, шаги и инструменты внутри остаются там.
- **Крупные полезные нагрузки не попадают в историю workflow.** Промпты, ввод-вывод инструментов и улики лежат во внешних хранилищах и входят по claim-check; workflow хранит состояние, команды и ссылки. Зафиксированные факты идут из транзакционного outbox в JetStream к проекциям, зеркалам и аналитике; REST, MCP и консоли читают read-модели Postgres, но никогда высокочастотные workflow-запросы.

## На практике

[Kardcraft](https://github.com/TekkenSteve/Kardcraft) — диалоговый продукт крафта флеш-карточек для интервального повторения — работает на этом дизайне: его графы производства карточек на LangGraph являются бэкендом AgentOS, а его Go-оркестратор задач — нижестоящим потребителем `agentos.PlanRuntime`. Вот мера чистоты границы: адаптер AgentOS в оркестраторе, по его собственному контракту, — *единственный пакет Kardcraft, который знает типы AgentOS*.

## Гарантии

Чек-лист, по которому платформенный ревьюер реально проходит проект:

- **Долговременность** — run'ы и plan'ы переживают сбои, деплои и многосуточные ожидания; `run.status` через Nexus — снапшот с ограниченным устареванием (≈5 c), никогда не блокирующий вызов бэкенда.
- **Независимость от бэкенда** — нативный, LangGraph, OpenCode-стиль, HTTP, gRPC и внешний Temporal стоят за одним контрактом; элементы батчей ограничены лимитами capability и не раздуваются в тысячи plan-узлов.
- **Управляемость** — опасная работа идёт через dry-run, оценку рисков, согласование, исполнение, отмену и компенсацию (`GovernedActionRuntime`).
- **Аудируемость** — решения, ссылки на улики, акторы и обоснования попадают в ledger; журнал аудита выстроен в хеш-цепочку и защищён от подмены.
- **Биллинг** — кредитные счета и журнал использования тарифицируют каждый run.
- **Мультитенантность** — изоляция account/project сквозная, включая строковые ключи тенантов в схеме.
- **Нейтральность к движку** — публичные контракты не содержат Temporal-импортов; Temporal целиком живёт в адаптере `agentos/temporal` и может быть заменён, не трогая ни одного потребителя.
- **Край интероперабельности** — `cmd/agentos-plan` валидирует plan'ы, выдаёт JSON Schema для авторинга и импортирует/экспортирует [Serverless Workflow](https://serverlessworkflow.io/) как формат обмена.

## Место в экосистеме

Каждый слой агентного стека сейчас на волне. Они решают разные задачи:

| Класс | Примеры | Единица сервиса | Что оптимизируют |
|---|---|---|---|
| Агенты-компаньоны | OpenClaw (🦞), Hermes, Muse, Cue, Grokbot | внимание одного человека | личность, личный контекст, чат-каналы |
| Продукты автономных задач | Manus | один результат задачи | сквозное исполнение в песочнице вендора |
| Coding-агентные harness'и | DeepSeek Harness, Claude Code, Codex | сессия одного разработчика | цикл агента: инструменты, песочница, ревью |
| Агентные фреймворки | LangGraph, CrewAI | код вашего приложения | построить одно агентное приложение |
| **GoAgent (AgentOS)** | **этот репозиторий** | **агентная нагрузка одного продукта** | долговременность, governance, аудит, биллинг, мультитенантность — как платформа, которой владеет продукт |

**GoAgent — это harness?** Нет. Harness — это кабина *одного* агентного цикла: он ведёт инструменты, песочницу и ревью для одного разработчика в терминале. GoAgent — диспетчерская служба для *множества* run'ов, принадлежащих бэкендам: жизненный цикл, governance и аудит всей нагрузки. Они сочетаются, а не конкурируют: агент, собранный в harness'е (или любой HTTP/gRPC/Temporal-сервис), подключается как один из бэкендов под control plane — именно так в него уже встроены LangGraph и OpenCode-подобные runtime. И GoAgent — не фреймворк, против которого вы пишете код в своём процессе; это инфраструктура, которую ваш продукт *использует — через REST, встраивание в Go или чисто типовые контракты.

## Быстрый старт

Нужны Go 1.26+, Docker и Docker Compose.

```sh
# Один раз: сгенерировать dev-учётные данные (.env). Без них стек
# откажется стартовать, и репозиторий их не поставляет.
make dev-secrets

# Поднять зависимости: Postgres, Redis, NATS JetStream, Centrifugo, Temporal
make compose-up

# Запустить приложение (сборка с тегом migrate и применением миграций)
make run
```

- REST API: `http://127.0.0.1:8080` — [`/healthz`](http://127.0.0.1:8080/healthz), [`/metrics`](http://127.0.0.1:8080/metrics), [`/swagger`](http://127.0.0.1:8080/swagger)
- Полный стек в Docker: `make compose-up-all`
- Интеграционные тесты (mock LLM, внутри контейнерной сети): `make compose-up-integration-test`

## Обзор API

REST версионируется под `/v1`; полный справочник — на [`/swagger`](http://127.0.0.1:8080/swagger). Форма такая:

| Область | Представительные эндпоинты |
|---|---|
| Run'ы | `POST /v1/agentos/runs` · `GET /runs/{id}/status` · `POST /runs/{id}/signals` · `POST /runs/{id}/control` |
| Долговременные plan'ы | `POST /v1/agentos/plans` · `GET /plans/{id}/status` · `GET /plans/{id}/events` (SSE) · `GET /plans/{id}/audits` · `GET /plans/{id}/artifacts/{id}` |
| Инструменты авторинга | `GET /v1/agentos/plans/schemas/{kind}` · `GET /v1/agentos/plans/author` · `cmd/agentos-plan` (validate / schema / импорт-экспорт Serverless Workflow) |
| Шаблоны | `POST /v1/templates/import` · `GET /v1/templates/` |

## Структура проекта

Небольшая публичная граница **AgentOS** плюс адаптеры и оболочка приложения. Пакеты реализаций в `internal/` не являются публичными контрактами.

```text
Публичные порты (нейтральны к движку, без Temporal-импортов)
  agentos/core       # signals, controls, events, artifacts, tools, errors
  agentos/control    # контракты run и plan, capabilities, backend refs
  agentos/process    # resources, ledger, governed actions, batches, projections
  agentos/platform   # фасад композиции, когда приложению нужны оба плана

Адаптер по умолчанию
  agentos/temporal   # Temporal + Postgres + Redis + artifact store
  agentos/nexusapi   # версионируемый контракт Nexus-сервиса

Оболочка приложения (не публична)
  internal/controller   # REST-транспорт
  internal/usecase      # сценарии приложения
  internal/repo         # персистентность (sqlc-first) и адаптеры бэкендов
  internal/agentfw      # нативный GoAgent-бэкенд — один бэкенд, а не вся архитектура
  internal/app, cmd/    # связывание и точки входа
```

Два правила сохраняют границу честной: `agentos/control` никогда не импортирует `agentos/process` (агентные run'ы не знают бизнес-семантики), а `agentos/process` никогда не импортирует `agentos/control` (долговременные процессы существуют и без исполнения агентов). SQL живёт в `internal/repo/persistent/queries/*.sql` и компилируется `make sqlc` в типобезопасные связки — без ORM и без сборки SQL в рантайме.

## Режимы использования

1. **Автономный сервер** — запустите GoAgent как сервис; ваш продукт работает с ним через REST control plane.
2. **Встраивание как Go-библиотеки** — импортируйте границу AgentOS и поднимите её адаптером по умолчанию:

```go
import (
    agentos "github.com/TekkenSteve/GoAgent/agentos/control"
    agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)

rt, _ := agentostemporal.NewRuntime(ctx, agentostemporal.RuntimeConfig{
    TemporalAddress:    "127.0.0.1:7233",
    TemporalNamespace:  "default",
    TemporalTaskQueues: agentostemporal.DefaultTaskQueues(),
    PostgresURL:        "postgres://goagent:goagent@127.0.0.1:5432/goagent?sslmode=disable",
    RedisURL:           "redis://127.0.0.1:6379/0",
})
status, _ := rt.Start(ctx, agentos.RunSpec{ /* … */ })
```

3. **Только публичные типы** — импортируйте `agentos/core` и `agentos/control` для общих контрактов.

Для каждого режима есть запускаемые примеры в [`examples/`](examples/) (REST, embed, types).

## Разработка

```sh
make sqlc                # перегенерировать типобезопасные SQL-связки (обязательно после правок query/schema)
make swag-v1             # перегенерировать Swagger-документацию
make mock                # перегенерировать gomock-моки
make test                # юнит-тесты (-race)
make compose-up-integration-test   # интеграционный набор внутри контейнерной сети
make linter-golangci     # линтер
make check-import-boundary        # нейтральность публичных контрактов к движку
make check-workflow-determinism   # детерминизм Temporal workflow
```

Конфигурация по 12-factor, только переменные окружения — см. [config/config.go](config/config.go) и [.env.example](.env.example). Миграции — пары golang-migrate в [`migrations/`](migrations/); приложение применяет их на старте при сборке с тегом `migrate`. Трейсинг — OpenTelemetry (`TRACING_ENABLED`), метрики — Prometheus, логи — zerolog.

## Перспективы

- **Ядро — всего лишь деталь.** Публичные контракты не называют движок исполнения; Temporal живёт в одном адаптере. Другое долговременное ядро можно подставить, не трогая ни одного потребителя.
- **Обещания вместо опросов.** Поверхность Nexus растёт к долговременным межсервисным операциям, которые держатся сквозь сбои секунды и дни, ровно один раз, вместо самодельных опросов статуса.
- **Факты текут в одну сторону.** Транзакционный outbox → хребет JetStream становится каноничным источником фактов: проекции, зеркала, аналитика и реплей подписываются; UI-дельты идут отдельно через Centrifugo и в стримы не попадают.
- **MCP как лингва франка инструментов.** Нативный бэкенд уже говорит на MCP; каталоги capability и artifact-schema — версионируемые JSON Schema: то, что нужно агентному маркетплейсу раньше, чем сами агенты.
- **Агенты как коммунальная услуга.** Финал — агентная работа, которой пользуются как электричеством: тарификация через ledger, защита от подмены через аудит-цепочку, ограждение через governed actions — скучные ровно настолько, насколько положено инфраструктуре.

## Ссылки

- [Temporal](https://temporal.io/) · [NATS JetStream](https://nats.io/) · [Centrifugo](https://centrifugal.dev/) · [sqlc](https://sqlc.dev/)
- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) · [The Twelve-Factor App](https://12factor.net/)

## Лицензия

MIT License — см. [LICENSE](LICENSE).

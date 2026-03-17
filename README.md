# Sankofa Processing Engine (Go)

The **Sankofa Engine** is a high-performance, Go-based analytics processing core. It handles everything from real-time event ingestion to complex SQL-based analytics across SQLite and ClickHouse.

---

## 🛠 Features

- **Blazing Fast Ingestion**: Supports batch and single-event tracking with asynchronous buffering to ClickHouse.
- **Hybrid DB Architecture**: Uses **SQLite** for metadata and project management, and **ClickHouse** for high-volume analytical data.
- **Comprehensive API**: Full support for funnels, flows, insights, and retention analytics.
- **Role-Based Access Control (RBAC)**: Fine-grained permissions (Viewer, Editor, Admin, Owner) for organizations and projects.

---

## 📂 Project Structure

- `cmd/sankofa`: Entry point for the main application.
- `internal/api`: REST handlers and route registrations.
- `internal/database`: GORM models for SQLite and ClickHouse schema migrations.
- `internal/middleware`: Authentication (JWT) and RBAC logic.
- `internal/registry`: Hooks for extending functionalities (e.g., Enterprise features).
- `internal/utils`: GeoIP, configuration, and shared helpers.

---

## 🚀 Development Setup

### 1. Prerequisites
- **Go 1.24+**
- **SQLite3**
- **ClickHouse** (optional for local metadata-only dev)

### 2. Configuration
Copy the example environment file and adjust your settings:
```bash
cp .env.example .env
```

### 3. Run Locally
```bash
go run cmd/sankofa/
```

The server will start on `:8080` by default.

---

## 📡 API Overview (v1)

All endpoints require a `Authorization: Bearer <JWT>` token, except for authentication and health checks.

- `POST /api/v1/track`: Track a single event.
- `POST /api/v1/batch`: Batch ingestion of events and person profiles.
- `GET /api/v1/insights`: Fetch saved analytics insights.
- `GET /api/v1/funnels/calculate`: On-the-fly funnel calculation.
- `GET /api/v1/people`: List and search through person profiles.

Refer to the internal codebase or documentation for detailed payload specifications.

---

## 📦 Deployment

The engine is best deployed using the unified Docker image found in the `sankofa-docker` repository. This ensures that all dependencies (ClickHouse, network configurations) are properly managed.

---

## 🛡 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

# BizPulse: Real-Time Sales & Demand Forecasting Dashboard

![Python](https://img.shields.io/badge/python-3.9-blue.svg)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=flat&logo=fastapi)
![MySQL](https://img.shields.io/badge/mysql-%2300f.svg?style=flat&logo=mysql&logoColor=white)
![Nginx](https://img.shields.io/badge/nginx-%23009639.svg?style=flat&logo=nginx&logoColor=white)

BizPulse is a full-stack, real-time business intelligence platform designed to help businesses prevent stockouts and optimize inventory. It ingests a live stream of sales data, updates inventory levels in real-time, generates actionable 30-day demand forecasts, and exposes a full REST API for inventory and supplier management — all served through a secure, Nginx-proxied web dashboard.

## Core Features

- **Real-Time Data Pipeline:** Simulates and processes a live stream of sales and inventory events using Apache Kafka.
- **Automated Demand Forecasting:** A scheduler service automatically runs a Prophet time-series model to generate fresh 30-day demand forecasts for every product.
- **Full Inventory Management:** Complete product CRUD (add, edit, delete), stock movement tracking, and bulk CSV import with a downloadable template.
- **Supplier & Purchase Order Management:** Create and manage suppliers, raise purchase orders, and track order status end-to-end.
- **Alert System:** Automated low-stock alerts with dismissal support and threshold-based auto-generation.
- **CSV Report Downloads:** Export inventory snapshots and sales data as CSV files directly from the dashboard.
- **Interactive Web Dashboard:** A secure, login-protected web interface built with FastAPI and modern JavaScript, featuring:
  - At-a-glance KPI cards for key business metrics.
  - A real-time, sortable view of current inventory levels.
  - Interactive charts to visualize demand forecasts for any product.
- **Nginx Reverse Proxy:** Production-ready Nginx configuration sitting in front of the FastAPI backend with rate limiting support.
- **Containerized Architecture:** The entire application stack (Kafka, Zookeeper, MySQL, Nginx, Stream Processor, Scheduler, Website) is orchestrated by Docker Compose for one-command setup and consistent deployment.
- **Robust & Modular Backend:** A clear, well-structured Python codebase separates concerns for data simulation, stream processing, forecasting, and the web API.

## Technology Stack

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Containerization** | Docker / Docker Compose | Orchestrates and runs all microservices. |
| **Reverse Proxy** | Nginx | Sits in front of FastAPI; handles routing and rate limiting. |
| **Data Streaming** | Apache Kafka | Manages the real-time sales data event stream. |
| **Stream Processing** | Python (kafka-python) | Consumes Kafka events and updates the database. |
| **Web Framework** | FastAPI | Serves the web dashboard and secure REST API. |
| **Database** | MySQL | Stores inventory, sales transactions, users, forecasts, suppliers, and orders. |
| **Predictive Model** | Python (Prophet) | Generates time-series demand forecasts. |
| **Data Handling** | Python (Pandas) | Prepares and aggregates data for the model. |
| **Scheduling** | Python Subprocess Loop | Triggers the forecasting script at a regular interval. |
| **Authentication** | JWT (python-jose) | Secures the API and dashboard endpoints. |
| **Frontend** | HTML, CSS, JavaScript | Provides an interactive and responsive user interface. |

## System Architecture

```
Sales Data Simulator
        │
        ▼
  Apache Kafka Topic
        │
        ▼
  Stream Processor ──────────────────────────────┐
        │                                         │
        ▼                                         ▼
     MySQL DB ◄──── Demand Forecaster ◄──── Scheduler
        │
        ▼
  FastAPI Backend
        │
        ▼
     Nginx Proxy
        │
        ▼
  Frontend Dashboard (Browser)
```

1. **Sales Data Simulator** generates historical and live sales events and sends them to a Kafka topic.
2. **Stream Processor** consumes events from Kafka, updating the `inventory` and `sales_transactions` tables in MySQL.
3. **Scheduler** periodically triggers the **Demand Forecaster**.
4. **Demand Forecaster** queries historical sales from MySQL, trains a Prophet model for each product, and saves predictions to the `forecast_predictions` table.
5. **FastAPI Web Server** provides a secure login and exposes REST API endpoints for inventory, products, suppliers, purchase orders, alerts, and reports.
6. **Nginx** proxies all incoming requests to FastAPI with rate limiting.
7. The **User** interacts with the dashboard to view inventory, manage products and suppliers, raise purchase orders, and download reports.

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
  - **Important:** Ensure Docker has sufficient memory allocated (**6 GB or more is recommended**). You can configure this in Docker Desktop's settings under `Resources`.
- [Python 3.9+](https://www.python.org/downloads/)
- [Git](https://git-scm.com/downloads)

### 1. Clone the Repository

```bash
git clone https://github.com/Retr0-c0der/BizPulse-Analytics-Platform.git
cd BizPulse-Analytics-Platform
```

### 2. Configure Environment Variables

Create a file named `.env` in the root directory. The default values are already configured to work with Docker Compose, but you can customize them if needed.

```env
# .env
DB_HOST=mysql_db
DB_DATABASE=bizpulse_db
DB_USER=bizpulse_user
DB_PASSWORD=userpassword

# --- Forecasting Parameters ---
FORECAST_DAYS=30
MIN_HISTORICAL_DAYS=14
MIN_TRANSACTIONS=5

# --- Security ---
SECRET_KEY=a_very_secret_key_that_should_be_in_your_env_file
```

### 3. Build and Run the Entire System

This single command will build the custom Docker images and start all services (database, Kafka, Nginx, backend services, and website) in the background. The database schema will be initialized automatically.

```bash
docker-compose up --build -d
```

*The `--build` flag ensures images are rebuilt if you've made code changes. The `-d` flag runs the containers in detached mode.*

### 4. Run the Data Simulation

The system is now running and waiting for data. You will need one separate terminal to run the data simulator.

First, set up a local Python virtual environment to run the simulator script:

```bash
# Create a virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (macOS/Linux)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

Now, start the simulator. It will first generate several days of historical data and then switch to sending live sales events.

```bash
python sales_data_simulator.py
```

Leave this terminal running.

### 5. Access the Dashboard

The system is now fully operational.

1. Open your web browser and navigate to **`http://localhost:80`**.
2. Log in with the default credentials:
   - **Username:** `testuser`
   - **Password:** `testpassword`

You will be redirected to the dashboard. The KPI cards and inventory table will populate immediately. The forecast chart will populate after the first scheduled forecast run completes (within a minute).

## API Overview

BizPulse exposes a REST API at `http://localhost:80/api/`. Key endpoint groups:

| Endpoint Group | Description |
| :--- | :--- |
| `/api/products` | Full product CRUD |
| `/api/inventory` | Inventory levels and stock movement tracking |
| `/api/suppliers` | Supplier management |
| `/api/purchase-orders` | Purchase order creation and status tracking |
| `/api/alerts` | Low-stock alert retrieval and dismissal |
| `/api/reports` | CSV report downloads |
| `/api/forecasts` | Demand forecast data per product |

Full interactive API docs available at **`http://localhost:80/docs`** (Swagger UI).

## How to Shut Down

To stop all running services and remove the containers, run:

```bash
docker-compose down
```

To also remove the database volume (deleting all data), add the `-v` flag:

```bash
docker-compose down -v
```

## Project Structure

```
BizPulse/
├── frontend/
│   ├── dashboard.html
│   ├── register.html
│   ├── app.js
│   └── style.css
├── main.py                  # FastAPI app and all API routes
├── demand_forecaster.py     # Prophet forecasting script
├── stream_processor.py      # Kafka consumer
├── sales_data_simulator.py  # Kafka producer / data simulator
├── docker-compose.yml
├── nginx.conf
├── db_setup.sql
├── migration.sql
├── requirements.txt
└── .env                     # (create this yourself — not committed)
```
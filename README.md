# BizPulse: Real-Time Sales & Inventory Analytics Platform

![Python](https://img.shields.io/badge/python-3.9-blue.svg)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![MySQL](https://img.shields.io/badge/mysql-%2300f.svg?style=flat&logo=mysql&logoColor=white)

BizPulse is a modular, cloud-ready prototype for a real-time business intelligence platform. It is designed to help small to medium enterprises (SMEs) prevent stockouts and overstocking by tracking sales in real-time and using machine learning to forecast future demand.

## Core Features
- **Real-Time Data Streaming:** Simulates a live stream of sales and inventory data using Apache Kafka.
- **Predictive Analytics:** Uses the Prophet time-series model to generate a 30-day demand forecast for each product.
- **Containerized Infrastructure:** The entire backend (Kafka, Zookeeper, MySQL) is managed by Docker and Docker Compose for easy setup and deployment.
- **Modular Python Codebase:** Clear separation between data simulation, machine learning, and database interactions.

## Technology Stack

| Component                | Technology         | Role                               |
| ------------------------ | ------------------ | ---------------------------------- |
| **Containerization**     | Docker / Docker Compose | Orchestrates and runs all services.  |
| **Data Streaming**       | Apache Kafka       | Manages the real-time data stream. |
| **Database**             | MySQL              | Stores transaction and forecast data. |
| **Predictive Model**     | Python (Prophet)   | Generates demand forecasts.        |
| **Data Handling**        | Python (Pandas)    | Manipulates data for the model.    |

## Setup and Installation

### Prerequisites
- [Docker](https://www.docker.com/products/docker-desktop/)
- [Python 3.8+](https://www.python.org/downloads/)
- [Git](https://git-scm.com/downloads)

### 1. Clone the Repository
```bash
git clone https://github.com/<Your-Username>/BizPulse-Analytics-Platform.git
cd BizPulse-Analytics-Platform
```

### 2. Set Up Environment Variables
Create a file named `.env` in the root directory and add your local database credentials.
```env
DB_HOST=localhost
DB_DATABASE=bizpulse_db
DB_USER=bizpulse_user
DB_PASSWORD=userpassword
```

### 3. Build and Run Services
Start the Kafka and MySQL containers in the background.
```bash
docker-compose up -d
```

### 4. Set Up Python Environment
Create and activate a virtual environment, then install the required packages.
```bash
# Create venv
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (macOS/Linux)
# source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 5. Initialize the Database
Run the SQL script to create the necessary tables.
```bash
docker exec -i mysql_db mysql -u bizpulse_user -puserpassword bizpulse_db < db_setup.sql
```

## How to Run the System

You will need two separate terminals (with the `.venv` activated in both).

**Terminal 1: Start the Data Simulator**
```bash
python sales_data_simulator.py
```

**Terminal 2: Run the Demand Forecaster**
This script will read from the database, train the model, and save the predictions.
```bash
python demand_forecaster.py
```
*(Note: For the first run, you may need to manually insert historical data into the `sales_transactions` table for the forecaster to have data to work with.)*
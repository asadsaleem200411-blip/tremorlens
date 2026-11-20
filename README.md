# TremorLens

**An interactive seismic-data dashboard for exploring, comparing, and analyzing earthquake records from USGS and EMSC.**

TremorLens combines a React web interface with a FastAPI backend and a PostgreSQL database. It brings global and regional earthquake exploration, aftershock analysis, catalogue comparison, and data-quality inspection into one application.

> **Current application:** React + FastAPI. The legacy Streamlit interface is not part of the active application and is intentionally excluded from this project documentation.

---

## Contents

- [Overview](#overview)
- [Dashboard Sections](#dashboard-sections)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Installation](#installation)
- [Environment Configuration](#environment-configuration)
- [Run Locally](#run-locally)
- [API Checks](#api-checks)
- [Performance and Caching](#performance-and-caching)
- [Database and ETL](#database-and-etl)
- [Production Deployment](#production-deployment)
- [Git and Security](#git-and-security)

## Overview

TremorLens works with earthquake catalogue data from:

- **USGS** — United States Geological Survey.
- **EMSC** — European-Mediterranean Seismological Centre.

The project includes a data-processing workflow and a web application for inspecting the resulting seismic data. The FastAPI service reads from the existing PostgreSQL database and provides data to the React frontend.

The API is an application/data-access layer. It does not run the ETL pipeline or write to the database.

## Dashboard Sections

The React dashboard includes:

- **Overview** — high-level dataset and seismic activity summary.
- **Live Earthquakes** — recent earthquake information.
- **Global Explorer** — interactive global event exploration.
- **Pakistan & Hindu Kush** — regional seismic exploration.
- **Aftershock Explorer** — analysis of aftershock activity.
- **Seismic Analysis** — charts and analytical views of earthquake characteristics.
- **Data Quality** — data-quality information and validation tools.
- **USGS vs EMSC** — comparison of the two earthquake catalogues.
- **Methodology** — explanation of the data and application workflow.

TremorLens is a data exploration and analysis dashboard. It is not an earthquake prediction or early-warning system.

## Architecture

```text
USGS / EMSC
     |
     v
 Existing ETL Pipeline
     |
     v
 PostgreSQL Database (Neon)
     |
     v
 FastAPI Backend
     |
     v
 React Frontend (Vite)
     |
     v
 TremorLens Dashboard
```

1. **Data sources:** earthquake catalogue records are obtained from USGS and EMSC.
2. **ETL:** existing project scripts extract, transform, validate, and load records.
3. **Database:** PostgreSQL stores the tables used by the application.
4. **Backend:** FastAPI exposes endpoints for the frontend.
5. **Frontend:** React presents the data and interactive visualizations.

## Technology Stack

| Layer | Technologies |
|---|---|
| Frontend | React, Vite, JavaScript |
| Maps | React Leaflet, Leaflet, OpenStreetMap |
| Charts and icons | Recharts, Lucide |
| Backend | Python, FastAPI, Uvicorn |
| Database | PostgreSQL (Neon) |
| Data processing | Existing Python ETL and analysis scripts |
| Data sources | USGS and EMSC |

## Project Structure

The main folders relevant to the current application are:

```text
TremorLens/
├── api/                         # FastAPI backend
│   ├── __init__.py
│   └── main.py
├── frontend/                    # React + Vite application
│   ├── src/
│   │   ├── components/
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   └── styles.css
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
├── etl/                         # Existing ETL scripts
├── eda/                         # Exploratory data analysis
├── data/                        # Local data files, if required
├── models/                      # Models and related project files
├── reports/                     # Reports and outputs
├── requirements-react-fastapi.txt
├── requirements.txt             # Keep if needed by retained project scripts
├── .env.example                 # Example configuration; no real secrets
├── .gitignore
└── README.md
```

The exact contents may vary with the project version. This README describes the React + FastAPI application, not every research or development file.

## Requirements

- Python version compatible with the packages in `requirements-react-fastapi.txt`.
- Node.js (LTS recommended) and npm.
- Access to the configured PostgreSQL database.
- Git, if cloning or contributing through GitHub.

## Installation

### 1. Clone the repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd <YOUR_PROJECT_FOLDER>
```

Replace the placeholders with your actual repository URL and folder name.

### 2. Create and activate a Python virtual environment

In Windows PowerShell, from the project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If the virtual environment already exists, activate it rather than creating it again.

### 3. Install backend dependencies

```powershell
pip install -r requirements-react-fastapi.txt
```

### 4. Install frontend dependencies

```powershell
cd frontend
npm install
cd ..
```

## Environment Configuration

Create a local `.env` file in the project root (the same folder as `api/`) and configure the database connection:

```env
DATABASE_URL=your_postgresql_connection_string
CORS_ORIGINS=http://localhost:5173
```

- `DATABASE_URL` is the connection string for your existing PostgreSQL database.
- `CORS_ORIGINS` lists the frontend origin or origins allowed to call the API. Separate multiple origins with commas.

Use the actual variable names required by the backend configuration. The example values above are placeholders.

**Never commit `.env`, passwords, API keys, or database credentials.** Do not put database credentials in frontend code or public Vite variables.

## Run Locally

Start the backend and frontend in two separate terminals.

### Terminal 1 — FastAPI backend

From the project root:

```powershell
.\.venv\Scripts\Activate.ps1
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

The API should be reachable at:

- Local API: `http://127.0.0.1:8000`
- Interactive API documentation: `http://127.0.0.1:8000/docs`

### Terminal 2 — React frontend

From the project root:

```powershell
cd frontend
npm run dev -- --host 0.0.0.0
```

Vite normally serves the development site at:

`http://localhost:5173`

To test from another device on the same Wi-Fi network, open the laptop's LAN IP address with port `5173`. The frontend must also be able to reach the backend through the laptop's LAN IP and port `8000`; allow the required ports through the computer's firewall if necessary.

## API Checks

With the backend running, open:

- Health check: `http://127.0.0.1:8000/api/health`
- Overview endpoint: `http://127.0.0.1:8000/api/overview`
- API documentation: `http://127.0.0.1:8000/docs`

The health endpoint should report that the database is connected. If it does not, check the local `.env` configuration, database availability, and required tables.

## Performance and Caching

The React frontend includes session-only API caching and request optimizations to reduce
unnecessary network traffic and improve dashboard responsiveness.

### API response cache

- Non-live API responses are cached in browser memory for the current page session.
- The cache uses **LRU (Least Recently Used) eviction** and is bounded to **24 entries**
  and approximately **24 MB** of serialized response data.
- Responses larger than the cache memory limit are not stored.
- Repeated requests for the same API path can reuse the cached response.
- Identical requests already in progress are shared instead of starting duplicate fetches.
- Live earthquake feed requests bypass the cache so the live feed can stay fresh.
- A manual refresh clears the in-memory API cache so the selected page can request fresh data.
- The cache is not persistent: reloading or closing the browser page starts a new session.

### Global Explorer loading

- Year and minimum-magnitude filters are applied by the backend for the Global Explorer.
- Filter changes are debounced by **250 milliseconds**; rapid slider movement does not trigger
  a request for every intermediate value.
- A newer filter selection cancels the previous in-progress request.
- Each settled filter selection requests one page of up to **3,000 most recent matching events**,
  rather than downloading the entire catalogue.
- The event counter reports the exact total number of matching catalogue records separately
  from the records loaded for the map and event table.

### Regional Explorer

The existing Pakistan & Hindu Kush Explorer data-loading and filtering path is retained.
Its regional charts and visualizations continue to use the regional catalogue data path.

## Database and ETL

The API expects the following existing PostgreSQL tables:

- `usgs_events`
- `emsc_events`
- `events_clean`
- `rejected_records`
- `ingest_runs`
- `quality_issues`

The API does **not** create these tables, run ETL jobs, or write data to the database. Keep using the project's existing ETL and ingestion/reconciliation scripts separately. Do not run schema migrations or destructive SQL just to add the React interface.

## Production Deployment

The frontend and backend can be deployed separately or served through one domain using a reverse proxy.

### Separate frontend and backend

- Set the frontend's `VITE_API_URL` to the public HTTPS URL of the FastAPI backend.
- Set the backend's `CORS_ORIGINS` to the exact deployed frontend origin.
- Use HTTPS for both frontend and backend.
- Store `DATABASE_URL` and other secrets in the backend hosting provider's environment/secret settings.

### Same-origin deployment

A reverse proxy can serve the frontend and forward API requests to FastAPI. Configure the frontend and proxy consistently so API requests reach the backend under the same domain.

### Build the frontend

From the `frontend` directory:

```bash
npm run build
```

Before considering deployment complete, verify the production build, API health endpoint, database connection, CORS configuration, and dashboard sections on the deployed site.

## Git and Security

The active GitHub application should contain the React frontend, FastAPI backend, and the supporting ETL, analysis, model, and report files that are required for the project.

For a React + FastAPI-only repository:

- Exclude the legacy Streamlit application folder and its Streamlit-only configuration from Git.
- Keep the local legacy files until you have confirmed they are no longer needed; excluding them from Git does not require deleting them from your computer.
- Keep `api/`, `frontend/`, and the required ETL and project-supporting files.
- Keep `.env`, `.venv/`, `frontend/node_modules/`, Python caches, and temporary test caches out of Git.
- Review `git status` before committing to ensure no secrets or unwanted files are staged.

---

## Project Goal

TremorLens aims to make seismic catalogue data easier to explore and understand by combining data engineering, database-backed APIs, interactive visualization, and seismic analysis in one application.

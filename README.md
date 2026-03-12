# WASDE Data Automation System

A Python-based desktop application that automates the processing of **WASDE (World Agricultural Supply and Demand Estimates)** reports — eliminating manual data handling and enabling direct Power BI dashboard integration for agricultural commodity analysis.

---

## Features

- **Dual Processing Modes** – Normal mode for full data runs; Monthly Update mode for incremental refreshes
- **ETL Pipeline** – Extracts and consolidates multi-year CSV reports (2021–2025), transforms data, and outputs analysis-ready files
- **Automated Calculations** – Computes Stocks-to-Use ratio and Total Supply estimates across 5 commodities: wheat, corn, soybeans, rice, and cotton
- **Power BI Integration** – Automatically opens and triggers dashboard refresh after processing
- **Modern GUI** – Material Design interface built with tkinter/ttk
- **Real-time Logging** – Color-coded log viewer with severity levels for monitoring and audit

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| GUI Framework | Python tkinter + ttk |
| Data Processing | Pandas |
| Visualization | Power BI (.pbix) |
| Concurrency | Python threading |
| Logging | Python logging module |

---

## Project Structure

```
WASDE-Data-Automation-System/
├── wasde_gui_enhanced.py     # Main application (GUI + processing logic)
├── run_wasde_enhanced.bat    # One-click launcher script
├── Dash.pbix                 # Power BI dashboard
├── 2021-2025/                # Historical WASDE CSV data by year
└── Project-dat/              # Processed output files
```

---

## Getting Started

### Prerequisites

- Python 3.8+
- Power BI Desktop (for dashboard visualization)

### Installation

```bash
# Clone the repository
git clone https://github.com/huukhang2423/WASDE-Data-Automation-System.git
cd WASDE-Data-Automation-System

# Install dependencies
pip install pandas openpyxl
```

### Run

```bash
# Option 1: Python directly
python wasde_gui_enhanced.py

# Option 2: Windows batch script
run_wasde_enhanced.bat
```

---

## How It Works

1. **File Discovery** – Scans the data folder for all WASDE CSV files across years
2. **Data Consolidation** – Merges multiple CSVs into a unified DataFrame using pandas
3. **Filtering** – Filters by report title and specific commodity attributes
4. **Transformation** – Derives new columns: `True Attribute`, `ForecastYearDate`
5. **Calculations** – Computes two key metrics:
   - **Stocks Ratio** = Ending Stocks ÷ (Exports + Domestic Total)
   - **Total Supply** = Beginning Stocks + Production
6. **Output** – Saves processed CSVs and opens Power BI for visualization

---

## Commodities Covered

| Commodity | Stocks Ratio | Total Supply Estimate | Total Supply Projection |
|-----------|:-----------:|:--------------------:|:-----------------------:|
| Wheat | ✓ | ✓ | ✓ |
| Corn | ✓ | ✓ | ✓ |
| Soybeans | ✓ | ✓ | ✓ |
| Rice | ✓ | ✓ | ✓ |
| Cotton | ✓ | ✓ | ✓ |

---

## Background

WASDE reports are published monthly by the USDA and are a critical reference for global agricultural market analysis. This tool was built to automate the repetitive data extraction and transformation process, enabling faster and more consistent analysis workflows.

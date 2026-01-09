# Eurostat MCP Server

A Model Context Protocol (MCP) server that provides tools to query [Eurostat](https://ec.europa.eu/eurostat) APIs for European statistics data.

## Features

- **Search datasets** - Find Eurostat datasets by keyword
- **Query data** - Retrieve data with flexible filtering options
- **Export to CSV** - Save data locally for further analysis
- **Support for both APIs**:
  - SDMX 3.0 API for regular datasets (GDP, unemployment, population, etc.)
  - SDMX 2.1 Comext API for trade/production datasets (DS-prefixed)

## Available Tools

| Tool | Description |
|------|-------------|
| `search_datasets` | Search for datasets by keyword |
| `get_dataset_info` | Get metadata and dimensions for a dataset |
| `query_eurostat_data` | Query data with filters (geo, time, etc.) |
| `get_available_values` | List valid values for a dimension |
| `list_popular_datasets` | Show commonly used datasets by category |
| `export_to_csv` | Export data to a local CSV file |
| `export_multiple_datasets` | Batch export multiple datasets |

## Installation

### Prerequisites

- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Install with uv (recommended)

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/eurostat-mcp.git
cd eurostat-mcp

# Install dependencies
uv pip install -r requirements.txt
```

### Install with pip

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/eurostat-mcp.git
cd eurostat-mcp

# Create virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

### Claude Desktop

Add the following to your Claude Desktop configuration file:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "eurostat": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/eurostat-mcp",
        "run",
        "server.py"
      ]
    }
  }
}
```

Or with Python directly:

```json
{
  "mcpServers": {
    "eurostat": {
      "command": "python",
      "args": ["/path/to/eurostat-mcp/server.py"]
    }
  }
}
```

### Claude Code (CLI)

Add to your Claude Code settings (`.claude/settings.json` in your project or `~/.claude/settings.json` globally):

```json
{
  "mcpServers": {
    "eurostat": {
      "command": "python",
      "args": ["/path/to/eurostat-mcp/server.py"]
    }
  }
}
```

## Usage Examples

### Search for datasets

```
Search for GDP datasets
→ search_datasets("GDP")
```

### Query unemployment data

```
Get monthly unemployment rate for Germany and France in 2024
→ query_eurostat_data(
    dataset_id="UNE_RT_M",
    geo="DE+FR",
    time_period="ge:2024"
  )
```

### Query international trade data (Comext)

```
Get German exports to China in 2024
→ query_eurostat_data(
    dataset_id="DS-059341",
    filters="REPORTER=DE&PARTNER=CN&FLOW=2",
    time_period="2024"
  )
```

### Export data to CSV

```
Export GDP data for EU countries
→ export_to_csv(
    dataset_id="NAMA_10_GDP",
    output_path="data/gdp_eu.csv",
    geo="EU27_2020",
    time_period="ge:2015"
  )
```

## Popular Datasets

### Economic
- `NAMA_10_GDP` - GDP and main components
- `NAMQ_10_GDP` - Quarterly GDP

### Labour Market
- `UNE_RT_M` - Monthly unemployment rate
- `UNE_RT_A` - Annual unemployment rate
- `LFSI_EMP_A` - Employment rates

### Population
- `DEMO_PJAN` - Population on 1 January
- `DEMO_GIND` - Population change

### Prices
- `PRC_HICP_MANR` - Monthly inflation rates (HICP)

### International Trade (Comext)
- `DS-059341` - Trade by HS2-4-6 product codes
- `DS-059331` - Trade by SITC codes

### Business Surveys
- `ei_bsin_q_r2` - Industry survey results (quarterly)
- `ei_bsin_m_r2` - Industry confidence indicator (monthly)

> **Note**: DS-prefixed datasets (Comext/Prodcom) require heavy filtering due to their size. Always specify REPORTER, PARTNER, PRODUCT, or other filters.

## API Reference

This server uses the official Eurostat APIs:
- [SDMX 3.0 Dissemination API](https://wikis.ec.europa.eu/display/EUROSTATHELP/API+Statistics+-+data+query)
- [Comext SDMX 2.1 API](https://wikis.ec.europa.eu/display/EUROSTATHELP/Comext+-+Statistical+regime)

## Troubleshooting

### "Data too large" error
Apply more filters to reduce the data size:
- Add `geo` parameter for specific countries
- Add `time_period` for date ranges
- For Comext datasets, specify `REPORTER`, `PARTNER`, `PRODUCT`, `FLOW`

### Timeout errors
Large datasets may timeout. Try:
- Using `last_n_observations` to limit results
- Adding more specific filters
- Using `export_to_csv` for large extractions

### Dataset not found
Use `search_datasets()` to find the correct dataset ID, or `list_popular_datasets()` to browse common datasets.

## License

MIT License - see [LICENSE](LICENSE) file.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- Data provided by [Eurostat](https://ec.europa.eu/eurostat)
- Built with [FastMCP](https://github.com/jlowin/fastmcp)

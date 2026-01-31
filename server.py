"""
Eurostat MCP Server

An MCP server that provides tools to query Eurostat APIs for European statistics data.
Supports both:
- SDMX 3.0 API for regular datasets (NAMA_10_GDP, UNE_RT_M, etc.)
- SDMX 2.1 Comext API for DS-prefixed trade/production datasets (DS-059341, etc.)
"""

import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Optional
import httpx
from mcp.server.fastmcp import FastMCP

# Configure logging to stderr (never stdout for stdio servers!)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("eurostat-mcp")

# Initialize the MCP server
mcp = FastMCP("eurostat")

# Eurostat API base URLs
# Regular datasets use the standard dissemination API
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination"
# DS-prefixed datasets (Comext trade data, Prodcom) use the comext API
COMEXT_BASE_URL = "https://ec.europa.eu/eurostat/api/comext/dissemination"

SDMX_BASE = f"{BASE_URL}/sdmx/3.0"
# Comext uses SDMX 2.1, not 3.0
COMEXT_SDMX_BASE = f"{COMEXT_BASE_URL}/sdmx/2.1"
CATALOGUE_BASE = f"{BASE_URL}/catalogue/v1.0"


def is_comext_dataset(dataset_id: str) -> bool:
    """Check if a dataset ID is a DS-prefixed Comext/Prodcom dataset."""
    return dataset_id.upper().startswith("DS-")


def get_sdmx_base(dataset_id: str) -> str:
    """Get the appropriate SDMX base URL for a dataset."""
    if is_comext_dataset(dataset_id):
        return COMEXT_SDMX_BASE
    return SDMX_BASE


def get_data_url(dataset_id: str) -> str:
    """Get the appropriate data URL for a dataset."""
    if is_comext_dataset(dataset_id):
        # SDMX 2.1 format for Comext
        return f"{COMEXT_SDMX_BASE}/data/{dataset_id}"
    # SDMX 3.0 format for regular datasets
    return f"{SDMX_BASE}/data/dataflow/ESTAT/{dataset_id}/1.0/*"


def get_structure_url(dataset_id: str) -> str:
    """Get the appropriate structure URL for a dataset."""
    if is_comext_dataset(dataset_id):
        # SDMX 2.1 format for Comext
        return f"{COMEXT_SDMX_BASE}/datastructure/ESTAT/DSD_{dataset_id}"
    # SDMX 3.0 format for regular datasets
    return f"{SDMX_BASE}/structure/dataflow/ESTAT/{dataset_id}"

# HTTP client timeout
TIMEOUT = 60.0


async def fetch_json(url: str, params: Optional[dict] = None) -> dict:
    """Fetch JSON data from a URL."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        logger.info(f"Fetching: {url}")
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()


async def fetch_text(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> str:
    """Fetch text/CSV data from a URL with automatic decompression."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        logger.info(f"Fetching: {url}")
        # Explicitly request no compression to avoid gzip issues
        request_headers = {"Accept-Encoding": "identity"}
        if headers:
            request_headers.update(headers)
        response = await client.get(url, params=params, headers=request_headers)
        response.raise_for_status()
        return response.text


def is_xml_response(data: str) -> bool:
    """Check if response data is XML (SDMX format)."""
    return data.strip().startswith("<?xml") or data.strip().startswith("<m:GenericData")


def parse_sdmx_xml_to_csv(xml_data: str) -> str:
    """
    Parse SDMX 2.1 XML response and convert to CSV format.

    The XML structure is:
    <m:GenericData>
      <m:DataSet>
        <g:Series>
          <g:SeriesKey>
            <g:Value id="product" value="0101"/>
            <g:Value id="partner" value="FR"/>
            ...
          </g:SeriesKey>
          <g:Obs>
            <g:ObsDimension value="2022"/>
            <g:ObsValue value="123.45"/>
          </g:Obs>
          ...
        </g:Series>
        ...
      </m:DataSet>
    </m:GenericData>
    """
    try:
        root = ET.fromstring(xml_data)

        # Define namespaces used in SDMX 2.1
        namespaces = {
            'm': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'g': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic',
            'c': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
        }

        # Collect all rows and determine all columns
        rows = []
        all_columns = set()

        # Find all Series elements
        for series in root.findall('.//g:Series', namespaces):
            # Extract series key dimensions
            series_dims = {}
            series_key = series.find('g:SeriesKey', namespaces)
            if series_key is not None:
                for value in series_key.findall('g:Value', namespaces):
                    dim_id = value.get('id', '').upper()
                    dim_value = value.get('value', '')
                    series_dims[dim_id] = dim_value
                    all_columns.add(dim_id)

            # Extract observations
            for obs in series.findall('g:Obs', namespaces):
                row = dict(series_dims)  # Copy series dimensions

                # Get time period
                obs_dim = obs.find('g:ObsDimension', namespaces)
                if obs_dim is not None:
                    row['TIME_PERIOD'] = obs_dim.get('value', '')
                    all_columns.add('TIME_PERIOD')

                # Get observation value
                obs_value = obs.find('g:ObsValue', namespaces)
                if obs_value is not None:
                    row['OBS_VALUE'] = obs_value.get('value', '')
                    all_columns.add('OBS_VALUE')

                # Get any attributes
                for attr in obs.findall('g:Attributes/g:Value', namespaces):
                    attr_id = attr.get('id', '').upper()
                    attr_value = attr.get('value', '')
                    row[attr_id] = attr_value
                    all_columns.add(attr_id)

                rows.append(row)

        if not rows:
            return "No data found in XML response"

        # Define preferred column order
        preferred_order = ['FREQ', 'REPORTER', 'PARTNER', 'PRODUCT', 'FLOW', 'INDICATORS', 'TIME_PERIOD', 'OBS_VALUE']

        # Sort columns: preferred first, then alphabetically for the rest
        columns = []
        for col in preferred_order:
            if col in all_columns:
                columns.append(col)
                all_columns.discard(col)
        columns.extend(sorted(all_columns))

        # Build CSV output
        output = StringIO()

        # Write header
        output.write(','.join(columns) + '\n')

        # Write rows
        for row in rows:
            values = []
            for col in columns:
                val = row.get(col, '')
                # Quote values that contain commas or quotes
                if ',' in str(val) or '"' in str(val):
                    val = '"' + str(val).replace('"', '""') + '"'
                values.append(str(val))
            output.write(','.join(values) + '\n')

        return output.getvalue()

    except ET.ParseError as e:
        logger.error(f"XML parsing error: {e}")
        return f"Error parsing XML: {str(e)}"
    except Exception as e:
        logger.error(f"Error converting XML to CSV: {e}")
        return f"Error converting XML to CSV: {str(e)}"


# Cache for TOC list to avoid repeated downloads
_toc_cache: dict = {"data": None, "timestamp": None}
_CACHE_TTL = 3600  # Cache for 1 hour

# Catalogue TOC API endpoint (simpler and more reliable than SDMX dataflows)
TOC_URL = f"{BASE_URL}/catalogue/toc/txt"


@mcp.tool()
async def search_datasets(query: str, limit: int = 20) -> str:
    """
    Search for Eurostat datasets by keyword.

    Args:
        query: Search term to find datasets (e.g., "GDP", "unemployment", "population")
        limit: Maximum number of results to return (default: 20)

    Returns:
        List of matching datasets with their IDs and descriptions
    """
    global _toc_cache

    try:
        # Check cache first
        cache_valid = (
            _toc_cache["data"] is not None and
            _toc_cache["timestamp"] is not None and
            (datetime.now() - _toc_cache["timestamp"]).total_seconds() < _CACHE_TTL
        )

        if cache_valid:
            logger.info("Using cached TOC list")
            return _search_in_toc(_toc_cache["data"], query, limit)

        # Fetch TOC using Catalogue API (much faster and more reliable)
        params = {"lang": "en"}
        headers = {"Accept-Encoding": "identity"}

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            logger.info(f"Fetching TOC from: {TOC_URL}")
            response = await client.get(TOC_URL, params=params, headers=headers)
            response.raise_for_status()
            toc_text = response.text

            # Parse and cache the TOC
            toc_entries = _parse_toc_to_list(toc_text)
            _toc_cache["data"] = toc_entries
            _toc_cache["timestamp"] = datetime.now()
            logger.info(f"Cached {len(toc_entries)} TOC entries")

            return _search_in_toc(toc_entries, query, limit)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error searching datasets: {e}")
        return f"Error searching datasets: HTTP {e.response.status_code}. Try `list_popular_datasets()` for common datasets."
    except httpx.TimeoutException:
        logger.error("Timeout fetching TOC")
        return "Search timed out. Try `list_popular_datasets()` for common datasets, or use `get_dataset_info(dataset_id)` if you know the dataset ID."
    except Exception as e:
        logger.error(f"Error searching datasets: {e}")
        error_msg = str(e) if str(e) else type(e).__name__
        return f"Error searching datasets: {error_msg}. Try `list_popular_datasets()` for common datasets."


def _parse_toc_to_list(toc_text: str) -> list:
    """Parse TOC TXT (tab-separated) into a list of (code, title, type) tuples."""
    entries = []
    try:
        lines = toc_text.strip().split('\n')
        # Skip header line
        for line in lines[1:]:
            parts = line.split('\t')
            if len(parts) >= 3:
                title = parts[0].strip().strip('"')
                code = parts[1].strip().strip('"')
                item_type = parts[2].strip().strip('"')
                # Only include datasets and tables, not folders
                if item_type in ('dataset', 'table') and code:
                    entries.append((code, title, item_type))
        return entries
    except Exception as e:
        logger.error(f"Error parsing TOC: {e}")
        return []


def _search_in_toc(toc_entries: list, query: str, limit: int) -> str:
    """Search through cached TOC list."""
    query_lower = query.lower()
    results = []

    for code, title, item_type in toc_entries:
        if query_lower in code.lower() or query_lower in title.lower():
            type_badge = "📊" if item_type == "dataset" else "📋"
            results.append(f"- {type_badge} **{code}**: {title}")
            if len(results) >= limit:
                break

    if not results:
        return f"No datasets found matching '{query}'. Try different keywords or use `list_popular_datasets()`."

    return f"Found {len(results)} datasets matching '{query}':\n\n" + "\n".join(results)


@mcp.tool()
async def get_dataset_info(dataset_id: str) -> str:
    """
    Get detailed information about a specific Eurostat dataset, including
    its dimensions and available filters.

    Args:
        dataset_id: The Eurostat dataset ID (e.g., "NAMA_10_GDP", "UNE_RT_M", "DS-059341")

    Returns:
        Dataset metadata including dimensions, time coverage, and description
    """
    try:
        is_comext = is_comext_dataset(dataset_id)

        if is_comext:
            # SDMX 2.1 format for Comext datasets
            url = f"{COMEXT_SDMX_BASE}/dataflow/ESTAT/{dataset_id}"
            headers = {"Accept": "application/xml"}

            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()

                # Parse XML response for Comext
                root = ET.fromstring(response.text)

                result_parts = [f"## Dataset: {dataset_id}\n"]
                result_parts.append("**Type**: Comext/Prodcom (Trade Data)\n")
                result_parts.append("**API**: SDMX 2.1 (requires heavy filtering)\n")

                # Extract name from XML
                for name_elem in root.iter():
                    if 'Name' in name_elem.tag and name_elem.text:
                        result_parts.append(f"**Name**: {name_elem.text}\n")
                        break

                result_parts.append("\n### Important Notes for Comext datasets:")
                result_parts.append("- Full datasets cannot be downloaded due to size")
                result_parts.append("- You MUST use filters (e.g., REPORTER, PARTNER, PRODUCT)")
                result_parts.append("- Use `startPeriod` and `endPeriod` for time filtering")
                result_parts.append("- Example filters: REPORTER=DE&PARTNER=FR&FLOW=1")

                return "\n".join(result_parts)
        else:
            # Try Catalogue API first for regular datasets (more reliable)
            catalogue_url = f"{CATALOGUE_BASE}/dataflow/{dataset_id}"

            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                logger.info(f"Getting dataset info from catalogue: {catalogue_url}")
                response = await client.get(catalogue_url)

                if response.status_code == 200:
                    data = response.json()
                    result_parts = [f"## Dataset: {dataset_id}\n"]

                    name = data.get("name", "")
                    if isinstance(name, dict):
                        name = name.get("en", str(name))
                    if name:
                        result_parts.append(f"**Name**: {name}\n")

                    description = data.get("description", "")
                    if isinstance(description, dict):
                        description = description.get("en", "")
                    if description:
                        result_parts.append(f"**Description**: {description}\n")

                    # Get dimensions from catalogue
                    dimensions = data.get("dimensions", [])
                    if dimensions:
                        result_parts.append("\n### Dimensions (filters you can use):\n")
                        for dim in dimensions:
                            if isinstance(dim, dict):
                                dim_id = dim.get("id", "Unknown")
                                dim_name = dim.get("name", "")
                                if isinstance(dim_name, dict):
                                    dim_name = dim_name.get("en", dim_id)
                                result_parts.append(f"- **{dim_id}**: {dim_name}")
                            else:
                                result_parts.append(f"- **{dim}**")

                    return "\n".join(result_parts)

                # Fallback to SDMX 2.1 structure endpoint (more reliable for many datasets)
                logger.info("Catalogue API failed, trying SDMX 2.1 structure endpoint")
                sdmx21_url = f"{BASE_URL}/sdmx/2.1/dataflow/ESTAT/{dataset_id}/1.0"
                headers = {"Accept": "application/xml"}

                response = await client.get(sdmx21_url, headers=headers)

                if response.status_code == 200:
                    # Parse XML response for SDMX 2.1
                    root = ET.fromstring(response.text)
                    result_parts = [f"## Dataset: {dataset_id}\n"]
                    result_parts.append("**API**: SDMX 2.1\n")

                    # Extract name from XML (look for Name elements)
                    for elem in root.iter():
                        if 'Name' in elem.tag and elem.text and 'xml:lang' in elem.attrib:
                            if elem.attrib.get('{http://www.w3.org/XML/1998/namespace}lang', '') == 'en':
                                result_parts.append(f"**Name**: {elem.text}\n")
                                break
                        elif 'Name' in elem.tag and elem.text:
                            result_parts.append(f"**Name**: {elem.text}\n")
                            break

                    # Extract annotations for additional info
                    for annotation in root.iter():
                        if 'AnnotationText' in annotation.tag and annotation.text:
                            text = annotation.text.strip()
                            if text and len(text) < 200:
                                result_parts.append(f"**Info**: {text}\n")
                                break

                    result_parts.append("\n### Usage Notes:")
                    result_parts.append("- Use `query_eurostat_data` to fetch data from this dataset")
                    result_parts.append("- Use `get_available_values` to see dimension values")
                    result_parts.append("- For large datasets, apply filters to avoid timeouts")

                    return "\n".join(result_parts)

                # Final fallback to SDMX 3.0 structure endpoint
                logger.info("SDMX 2.1 failed, trying SDMX 3.0 structure endpoint")
                sdmx_url = f"{SDMX_BASE}/structure/dataflow/ESTAT/{dataset_id}?references=all"
                headers = {"Accept": "application/vnd.sdmx.structure+json;version=2.0.0"}

                response = await client.get(sdmx_url, headers=headers)
                response.raise_for_status()
                data = response.json()

                result_parts = [f"## Dataset: {dataset_id}\n"]

                if "data" in data:
                    structures = data.get("data", {}).get("dataflows", [])
                    if structures:
                        flow = structures[0]
                        name = flow.get("name", "")
                        if isinstance(name, dict):
                            name = name.get("en", str(name))
                        result_parts.append(f"**Name**: {name}\n")

                # Get dimension info from the data structure definition
                dsds = data.get("data", {}).get("dataStructures", [])
                if dsds:
                    dsd = dsds[0]
                    components = dsd.get("dataStructureComponents", {})
                    dims = components.get("dimensionList", {}).get("dimensions", [])

                    if dims:
                        result_parts.append("\n### Dimensions (filters you can use):\n")
                        for dim in dims:
                            dim_id = dim.get("id", "Unknown")
                            dim_name = dim.get("name", "")
                            if isinstance(dim_name, dict):
                                dim_name = dim_name.get("en", dim_id)
                            result_parts.append(f"- **{dim_id}**: {dim_name}")

                return "\n".join(result_parts)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error getting dataset info: {e}")
        return f"Error getting dataset info: {e.response.status_code} - Dataset '{dataset_id}' may not exist"
    except Exception as e:
        logger.error(f"Error getting dataset info: {e}")
        return f"Error getting dataset info: {str(e)}"


async def _query_sdmx21(
    client: httpx.AsyncClient,
    dataset_id: str,
    filters: Optional[str],
    time_period: Optional[str],
    geo: Optional[str],
    last_n_observations: Optional[int],
    format: str,
    use_comext_base: bool = False
) -> str:
    """Query data using SDMX 2.1 API."""
    params = {}
    headers = {"Accept-Encoding": "identity"}

    # For SDMX 2.1, we use a simpler URL format with query params
    if use_comext_base:
        base_url = COMEXT_SDMX_BASE
    else:
        base_url = f"{BASE_URL}/sdmx/2.1"

    url = f"{base_url}/data/{dataset_id}"

    # Time period params for SDMX 2.1
    if time_period:
        if "-" in time_period and not time_period.startswith(("ge:", "le:")):
            parts = time_period.split("-")
            if len(parts) == 2:
                params["startPeriod"] = parts[0]
                params["endPeriod"] = parts[1]
        elif time_period.startswith("ge:"):
            params["startPeriod"] = time_period[3:]
        elif time_period.startswith("le:"):
            params["endPeriod"] = time_period[3:]
        else:
            params["startPeriod"] = time_period
            params["endPeriod"] = time_period

    if last_n_observations:
        params["lastNObservations"] = last_n_observations

    if format == "csv":
        headers["Accept"] = "application/vnd.sdmx.data+csv;file=true"
    else:
        headers["Accept"] = "application/json"

    logger.info(f"Querying SDMX 2.1: {url} with params: {params}")
    response = await client.get(url, params=params, headers=headers)
    response.raise_for_status()

    if format == "csv":
        data = response.text

        # Check if we got XML instead of CSV
        if is_xml_response(data):
            logger.info("Received XML response, converting to CSV...")
            data = parse_sdmx_xml_to_csv(data)
            if data.startswith("Error") or data.startswith("No data"):
                return data

        # Limit output size for readability
        lines = data.split("\n")
        if len(lines) > 100:
            preview = "\n".join(lines[:100])
            return f"{preview}\n\n... ({len(lines) - 100} more rows, total {len(lines)} rows)"
        return data
    else:
        return str(response.json())


@mcp.tool()
async def query_eurostat_data(
    dataset_id: str,
    filters: Optional[str] = None,
    time_period: Optional[str] = None,
    geo: Optional[str] = None,
    last_n_observations: Optional[int] = None,
    format: str = "csv"
) -> str:
    """
    Query data from a Eurostat dataset with optional filters.

    Args:
        dataset_id: The Eurostat dataset ID (e.g., "NAMA_10_GDP", "UNE_RT_M", "DS-059341")
        filters: Dimension filters in format "DIMENSION=value,value2" separated by "&"
                 Example: "FREQ=A&UNIT=PC_GDP" or "REPORTER=DE&PARTNER=FR&FLOW=1"
                 For multiple values in one dimension, use + (e.g., "GEO=DE+FR")
        time_period: Time period filter. For regular datasets: "2020", "ge:2015", "ge:2015+le:2023"
                     For Comext (DS-) datasets: use format "2020" or "2015-2023" (converted to startPeriod/endPeriod)
        geo: Geographic filter - country codes (e.g., "DE", "FR", or "DE+FR" for multiple)
        last_n_observations: Return only the last N observations per series
        format: Output format - "csv" (default) or "json"

    Returns:
        The requested data in the specified format
    """
    try:
        is_comext = is_comext_dataset(dataset_id)

        params = {}
        # Request no compression to avoid gzip decoding issues
        headers = {"Accept-Encoding": "identity"}

        if is_comext:
            # SDMX 2.1 format for Comext uses key-based path
            # Build key from filters - format: dim1.dim2.dim3...
            # Empty positions act as wildcards
            key_parts = []
            filter_dict = {}

            if filters:
                for filter_part in filters.split("&"):
                    if "=" in filter_part:
                        k, v = filter_part.split("=", 1)
                        # Replace comma with + for multiple values (SDMX 2.1 format)
                        v = v.replace(",", "+")
                        filter_dict[k.upper()] = v

            if geo:
                # Replace comma with + for multiple values
                filter_dict["REPORTER"] = geo.replace(",", "+")

            # Build the key in a standard order for Comext trade data
            # Typical order: FREQ.REPORTER.PARTNER.PRODUCT.FLOW.INDICATORS
            # We'll use wildcards (empty) for dimensions not specified
            comext_dims = ["FREQ", "REPORTER", "PARTNER", "PRODUCT", "FLOW", "INDICATORS"]
            for dim in comext_dims:
                key_parts.append(filter_dict.get(dim, ""))

            key = ".".join(key_parts)

            # Build URL - encode + as %2B for proper SDMX 2.1 multi-value syntax
            # The + character in SDMX key paths means "OR" (multiple values)
            encoded_key = key.replace("+", "%2B")
            url = f"{COMEXT_SDMX_BASE}/data/{dataset_id}/{encoded_key}"

            # Time period params for SDMX 2.1
            if time_period:
                if "-" in time_period and not time_period.startswith(("ge:", "le:")):
                    parts = time_period.split("-")
                    if len(parts) == 2:
                        params["startPeriod"] = parts[0]
                        params["endPeriod"] = parts[1]
                else:
                    params["startPeriod"] = time_period
                    params["endPeriod"] = time_period

            if format == "csv":
                headers["Accept"] = "application/vnd.sdmx.data+csv;file=true"
            else:
                headers["Accept"] = "application/json"

            if last_n_observations:
                params["lastNObservations"] = last_n_observations

            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                logger.info(f"Querying Comext: {url} with params: {params}")
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()

                if format == "csv":
                    data = response.text

                    # Check if we got XML instead of CSV (common for Comext datasets)
                    if is_xml_response(data):
                        logger.info("Received XML response, converting to CSV...")
                        data = parse_sdmx_xml_to_csv(data)
                        if data.startswith("Error") or data.startswith("No data"):
                            return data

                    # Limit output size for readability
                    lines = data.split("\n")
                    if len(lines) > 100:
                        preview = "\n".join(lines[:100])
                        return f"{preview}\n\n... ({len(lines) - 100} more rows, total {len(lines)} rows)"
                    return data
                else:
                    return str(response.json())
        else:
            # Try SDMX 3.0 format first for regular datasets
            url = f"{SDMX_BASE}/data/dataflow/ESTAT/{dataset_id}/1.0/*"

            # Add filters using c[DIMENSION] format
            if filters:
                for filter_part in filters.split("&"):
                    if "=" in filter_part:
                        key, value = filter_part.split("=", 1)
                        params[f"c[{key}]"] = value

            if time_period:
                if time_period.startswith(("ge:", "le:", "gt:", "lt:")):
                    params["c[TIME_PERIOD]"] = time_period
                else:
                    params["c[TIME_PERIOD]"] = f"eq:{time_period}"

            if geo:
                params["c[GEO]"] = geo

            if format == "csv":
                headers["Accept"] = "application/vnd.sdmx.data+csv;version=2.0.0;labels=name"
            else:
                headers["Accept"] = "application/json"

            if last_n_observations:
                params["lastNObservations"] = last_n_observations

            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                logger.info(f"Querying SDMX 3.0: {url} with params: {params}")
                response = await client.get(url, params=params, headers=headers)

                # If SDMX 3.0 fails with 400, try SDMX 2.1 as fallback
                if response.status_code == 400:
                    logger.info("SDMX 3.0 returned 400, falling back to SDMX 2.1")
                    return await _query_sdmx21(
                        client, dataset_id, filters, time_period, geo,
                        last_n_observations, format, use_comext_base=False
                    )

                response.raise_for_status()

                if format == "csv":
                    data = response.text

                    # Check if we got XML instead of CSV
                    if is_xml_response(data):
                        logger.info("Received XML response, converting to CSV...")
                        data = parse_sdmx_xml_to_csv(data)
                        if data.startswith("Error") or data.startswith("No data"):
                            return data

                    # Limit output size for readability
                    lines = data.split("\n")
                    if len(lines) > 100:
                        preview = "\n".join(lines[:100])
                        return f"{preview}\n\n... ({len(lines) - 100} more rows, total {len(lines)} rows)"
                    return data
                else:
                    return str(response.json())

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error querying data: {e}")
        if e.response.status_code == 404:
            return f"Dataset '{dataset_id}' not found. Use search_datasets to find valid dataset IDs."
        elif e.response.status_code == 413:
            return f"Error: Data too large. Apply more filters (GEO, TIME_PERIOD, etc.) to reduce the data size."
        return f"Error querying data: {e.response.status_code}"
    except Exception as e:
        logger.error(f"Error querying data: {e}")
        return f"Error querying data: {str(e)}"


@mcp.tool()
async def get_available_values(dataset_id: str, dimension: str) -> str:
    """
    Get the available values for a specific dimension in a dataset.
    Useful for understanding what filter values you can use.

    Args:
        dataset_id: The Eurostat dataset ID (e.g., "NAMA_10_GDP")
        dimension: The dimension ID to get values for (e.g., "GEO", "UNIT", "NA_ITEM")

    Returns:
        List of available values for the dimension with their labels
    """
    is_comext = is_comext_dataset(dataset_id)
    dimension_upper = dimension.upper()

    # For Comext datasets, provide common dimension values directly
    if is_comext:
        comext_dimensions = {
            "FREQ": [
                ("A", "Annual"),
                ("M", "Monthly"),
            ],
            "FLOW": [
                ("1", "Import"),
                ("2", "Export"),
            ],
            "INDICATORS": [
                ("VALUE_EUR", "Value in EUR"),
                ("VALUE_NAC", "Value in national currency"),
                ("QUANTITY_KG", "Quantity in kilograms"),
                ("SUP_QUANTITY", "Supplementary quantity"),
            ],
            "REPORTER": [
                ("AT", "Austria"), ("BE", "Belgium"), ("BG", "Bulgaria"),
                ("CY", "Cyprus"), ("CZ", "Czechia"), ("DE", "Germany"),
                ("DK", "Denmark"), ("EE", "Estonia"), ("EL", "Greece"),
                ("ES", "Spain"), ("FI", "Finland"), ("FR", "France"),
                ("HR", "Croatia"), ("HU", "Hungary"), ("IE", "Ireland"),
                ("IT", "Italy"), ("LT", "Lithuania"), ("LU", "Luxembourg"),
                ("LV", "Latvia"), ("MT", "Malta"), ("NL", "Netherlands"),
                ("PL", "Poland"), ("PT", "Portugal"), ("RO", "Romania"),
                ("SE", "Sweden"), ("SI", "Slovenia"), ("SK", "Slovakia"),
                ("EU27_2020", "EU 27 countries (from 2020)"),
            ],
            "PARTNER": [
                ("WORLD", "World (all countries)"),
                ("EU27_2020", "EU 27 countries"),
                ("EXTRA_EU27_2020", "Extra-EU27"),
                ("US", "United States"), ("CN", "China"), ("JP", "Japan"),
                ("GB", "United Kingdom"), ("CH", "Switzerland"), ("RU", "Russia"),
                ("IN", "India"), ("BR", "Brazil"), ("KR", "South Korea"),
                ("TR", "Turkey"), ("MX", "Mexico"), ("CA", "Canada"),
            ],
            "PRODUCT": [
                ("01", "Live animals"),
                ("02", "Meat and edible meat offal"),
                ("27", "Mineral fuels, oils and products"),
                ("84", "Machinery and mechanical appliances"),
                ("85", "Electrical machinery and equipment"),
                ("87", "Vehicles other than railway"),
                ("TOTAL", "Total all products"),
            ],
        }

        if dimension_upper in comext_dimensions:
            results = [f"## Available values for {dimension_upper} (Comext dataset):\n"]
            for code_id, name in comext_dimensions[dimension_upper]:
                results.append(f"- **{code_id}**: {name}")
            results.append("\n*Note: This is a subset of common values. For product codes, refer to HS nomenclature.*")
            return "\n".join(results)
        else:
            available_dims = ", ".join(comext_dimensions.keys())
            return f"Dimension '{dimension}' not found. Available Comext dimensions: {available_dims}"

    # For regular datasets, try multiple approaches
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            # Try SDMX 3.0 codelist endpoint first
            url = f"{SDMX_BASE}/structure/codelist/ESTAT/{dimension_upper}"
            headers = {"Accept": "application/vnd.sdmx.structure+json;version=2.0.0"}

            logger.info(f"Fetching codelist: {url}")
            response = await client.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                codelists = data.get("data", {}).get("codelists", [])
                if codelists:
                    codes = codelists[0].get("codes", [])
                    if codes:
                        results = [f"## Available values for {dimension_upper}:\n"]
                        for code in codes[:50]:
                            code_id = code.get("id", "Unknown")
                            name = code.get("name", "")
                            if isinstance(name, dict):
                                name = name.get("en", code_id)
                            results.append(f"- **{code_id}**: {name}")

                        if len(codes) > 50:
                            results.append(f"\n... and {len(codes) - 50} more values")
                        return "\n".join(results)

            # Try to get values from the dataset's available data
            # Using the availability endpoint
            avail_url = f"{SDMX_BASE}/data/dataflow/ESTAT/{dataset_id}/1.0/*?c[{dimension_upper}]=*&detail=nodata"
            logger.info(f"Trying availability endpoint: {avail_url}")
            response = await client.get(avail_url, headers={"Accept": "application/json"})

            if response.status_code == 200:
                data = response.json()
                # Extract dimension values from response structure
                dimensions = data.get("data", {}).get("structure", {}).get("dimensions", {})
                for dim_list in [dimensions.get("observation", []), dimensions.get("series", [])]:
                    for dim in dim_list:
                        if dim.get("id", "").upper() == dimension_upper:
                            values = dim.get("values", [])
                            if values:
                                results = [f"## Available values for {dimension_upper} in {dataset_id}:\n"]
                                for val in values[:50]:
                                    val_id = val.get("id", "Unknown")
                                    val_name = val.get("name", val_id)
                                    results.append(f"- **{val_id}**: {val_name}")
                                if len(values) > 50:
                                    results.append(f"\n... and {len(values) - 50} more values")
                                return "\n".join(results)

        return f"Could not find values for dimension '{dimension}' in dataset '{dataset_id}'. Try common dimensions like GEO, TIME_PERIOD, FREQ, UNIT."

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error getting dimension values: {e}")
        return f"Error: Could not fetch values for dimension '{dimension}'. Try: GEO, TIME_PERIOD, FREQ, UNIT"
    except Exception as e:
        logger.error(f"Error getting dimension values: {e}")
        return f"Error getting dimension values: {str(e)}"


@mcp.tool()
async def list_popular_datasets() -> str:
    """
    List commonly used Eurostat datasets across different statistical domains.
    Use this to discover available data without searching.

    Returns:
        A curated list of popular datasets organized by category
    """
    datasets = """## Popular Eurostat Datasets

### National Accounts (GDP, Economic)
- **NAMA_10_GDP**: GDP and main components (current prices, chain-linked volumes)
- **NAMA_10_PC**: GDP per capita
- **NAMQ_10_GDP**: Quarterly GDP and main components

### Employment & Unemployment
- **UNE_RT_M**: Monthly unemployment rate by sex and age
- **UNE_RT_A**: Annual unemployment rate
- **LFSI_EMP_A**: Employment rates by sex, age and citizenship

### Population & Demographics
- **DEMO_PJAN**: Population on 1 January by age and sex
- **DEMO_GIND**: Population change - demographic balance and crude rates
- **DEMO_MLEXPEC**: Life expectancy by age and sex

### Prices & Inflation
- **PRC_HICP_MANR**: HICP - monthly inflation rates
- **PRC_HICP_AIND**: HICP - annual average indices

### Trade & Balance of Payments
- **BOP_EU6_Q**: Balance of payments - quarterly data
- **DS-059341**: International trade of EU by HS2-4-6 (Comext)
- **DS-059331**: International trade by SITC (Comext)

### Production Statistics (Prodcom)
- **DS-059359**: Total production
- **DS-059358**: Sold production, exports and imports

**Note**: DS-prefixed datasets (Comext/Prodcom) require heavy filtering due to size.
Use filters like: FREQ=A&REPORTER=DE&PARTNER=FR&PRODUCT=0101&FLOW=1

### Energy & Environment
- **NRG_BAL_C**: Complete energy balances
- **ENV_AIR_GGE**: Greenhouse gas emissions

### Government Finance
- **GOV_10DD_EDPT1**: Government deficit and debt
- **GOV_10A_EXP**: Government expenditure by function

Use `get_dataset_info(dataset_id)` to see dimensions and filters for any dataset.
Use `search_datasets(query)` to find more specific datasets."""

    return datasets


@mcp.tool()
async def export_to_csv(
    dataset_id: str,
    output_path: str,
    filters: Optional[str] = None,
    time_period: Optional[str] = None,
    geo: Optional[str] = None,
    last_n_observations: Optional[int] = None,
    include_labels: bool = True
) -> str:
    """
    Export Eurostat data to a CSV file on disk.

    Args:
        dataset_id: The Eurostat dataset ID (e.g., "NAMA_10_GDP", "UNE_RT_M", "DS-059341")
        output_path: Path where the CSV file will be saved (e.g., "data/gdp_data.csv")
        filters: Dimension filters in format "DIMENSION=value,value2" separated by "&"
                 Example: "FREQ=A&UNIT=PC_GDP" or "FREQ=A&REPORTER=DE&PARTNER=FR&PRODUCT=0101&FLOW=1"
        time_period: Time period filter. For regular datasets: "2020", "ge:2015", "ge:2015+le:2023"
                     For Comext (DS-) datasets: use format "2020" or "2015-2023"
        geo: Geographic filter - country codes (e.g., "DE", "FR", or "DE+FR" for multiple)
        last_n_observations: Return only the last N observations per series
        include_labels: If True, use human-readable labels; if False, use codes only

    Returns:
        Confirmation message with file path and row count
    """
    try:
        is_comext = is_comext_dataset(dataset_id)

        params = {}
        # Request no compression to avoid gzip decoding issues
        headers = {"Accept-Encoding": "identity"}

        if is_comext:
            # SDMX 2.1 format for Comext uses key-based path
            filter_dict = {}

            if filters:
                for filter_part in filters.split("&"):
                    if "=" in filter_part:
                        k, v = filter_part.split("=", 1)
                        v = v.replace(",", "+")
                        filter_dict[k.upper()] = v

            if geo:
                filter_dict["REPORTER"] = geo.replace(",", "+")

            # Build the key in standard order for Comext trade data
            comext_dims = ["FREQ", "REPORTER", "PARTNER", "PRODUCT", "FLOW", "INDICATORS"]
            key_parts = [filter_dict.get(dim, "") for dim in comext_dims]
            key = ".".join(key_parts)

            # Encode + as %2B for proper SDMX 2.1 multi-value syntax
            encoded_key = key.replace("+", "%2B")
            url = f"{COMEXT_SDMX_BASE}/data/{dataset_id}/{encoded_key}"

            # Time period params for SDMX 2.1
            if time_period:
                if "-" in time_period and not time_period.startswith(("ge:", "le:")):
                    parts = time_period.split("-")
                    if len(parts) == 2:
                        params["startPeriod"] = parts[0]
                        params["endPeriod"] = parts[1]
                else:
                    params["startPeriod"] = time_period
                    params["endPeriod"] = time_period

            headers["Accept"] = "application/vnd.sdmx.data+csv;file=true"
        else:
            # SDMX 3.0 format for regular datasets
            url = f"{SDMX_BASE}/data/dataflow/ESTAT/{dataset_id}/1.0/*"

            if filters:
                for filter_part in filters.split("&"):
                    if "=" in filter_part:
                        key, value = filter_part.split("=", 1)
                        params[f"c[{key}]"] = value

            if time_period:
                if time_period.startswith(("ge:", "le:", "gt:", "lt:")):
                    params["c[TIME_PERIOD]"] = time_period
                else:
                    params["c[TIME_PERIOD]"] = f"eq:{time_period}"

            if geo:
                params["c[GEO]"] = geo

            label_type = "name" if include_labels else "id"
            headers["Accept"] = f"application/vnd.sdmx.data+csv;version=2.0.0;labels={label_type}"

        if last_n_observations:
            params["lastNObservations"] = last_n_observations

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            logger.info(f"Exporting: {url} with params: {params}")
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            csv_data = response.text

        # Check if we got XML instead of CSV (common for Comext datasets)
        if is_xml_response(csv_data):
            logger.info("Received XML response, converting to CSV...")
            csv_data = parse_sdmx_xml_to_csv(csv_data)
            if csv_data.startswith("Error") or csv_data.startswith("No data"):
                return csv_data

        # Create output directory if it doesn't exist
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Write CSV to file
        with open(output_file, "w", encoding="utf-8", newline="") as f:
            f.write(csv_data)

        # Count rows (excluding header)
        row_count = len(csv_data.strip().split("\n")) - 1

        # Get file size
        file_size = output_file.stat().st_size
        if file_size > 1024 * 1024:
            size_str = f"{file_size / (1024 * 1024):.2f} MB"
        elif file_size > 1024:
            size_str = f"{file_size / 1024:.2f} KB"
        else:
            size_str = f"{file_size} bytes"

        return f"Successfully exported data to: {output_file.absolute()}\n- Rows: {row_count}\n- Size: {size_str}\n- Dataset: {dataset_id}"

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error exporting data: {e}")
        if e.response.status_code == 404:
            return f"Dataset '{dataset_id}' not found. Use search_datasets to find valid dataset IDs."
        elif e.response.status_code == 413:
            return f"Error: Data too large. For Comext (DS-) datasets, you MUST apply more filters (REPORTER, PARTNER, PRODUCT, FLOW, etc.)"
        return f"Error exporting data: {e.response.status_code}"
    except PermissionError:
        return f"Permission denied: Cannot write to '{output_path}'. Check file permissions."
    except Exception as e:
        logger.error(f"Error exporting data: {e}")
        return f"Error exporting data: {str(e)}"


@mcp.tool()
async def export_multiple_datasets(
    datasets: str,
    output_directory: str,
    time_period: Optional[str] = None,
    geo: Optional[str] = None
) -> str:
    """
    Export multiple Eurostat datasets to CSV files in a directory.

    Args:
        datasets: Comma-separated list of dataset IDs (e.g., "NAMA_10_GDP,UNE_RT_A,DEMO_PJAN")
        output_directory: Directory where CSV files will be saved
        time_period: Time period filter applied to all datasets (e.g., "ge:2015+le:2023" or "2015-2023")
        geo: Geographic filter applied to all datasets (e.g., "DE,FR,IT,ES")

    Returns:
        Summary of exported files
    """
    dataset_list = [d.strip() for d in datasets.split(",")]
    output_dir = Path(output_directory)
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    successful = 0
    failed = 0

    for dataset_id in dataset_list:
        try:
            is_comext = is_comext_dataset(dataset_id)

            # Build filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d")
            filename = f"{dataset_id}_{timestamp}.csv"
            output_path = output_dir / filename

            # Build the URL (use appropriate format for dataset type)
            if is_comext:
                url = f"{COMEXT_SDMX_BASE}/data/{dataset_id}"
            else:
                url = f"{SDMX_BASE}/data/dataflow/ESTAT/{dataset_id}/1.0/*"

            params = {}

            if time_period:
                if is_comext:
                    # SDMX 2.1 uses startPeriod/endPeriod
                    if "-" in time_period and not time_period.startswith(("ge:", "le:")):
                        parts = time_period.split("-")
                        if len(parts) == 2:
                            params["startPeriod"] = parts[0]
                            params["endPeriod"] = parts[1]
                    else:
                        params["startPeriod"] = time_period
                        params["endPeriod"] = time_period
                else:
                    if time_period.startswith(("ge:", "le:", "gt:", "lt:")):
                        params["c[TIME_PERIOD]"] = time_period
                    else:
                        params["c[TIME_PERIOD]"] = f"eq:{time_period}"

            if geo:
                if is_comext:
                    params["c[REPORTER]"] = geo
                else:
                    params["c[GEO]"] = geo

            # Request no compression to avoid gzip decoding issues
            headers = {"Accept-Encoding": "identity"}
            if is_comext:
                headers["Accept"] = "application/vnd.sdmx.data+csv;file=true"
            else:
                headers["Accept"] = "application/vnd.sdmx.data+csv;version=2.0.0;labels=name"

            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                logger.info(f"Exporting {dataset_id}...")
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                csv_data = response.text

            # Check if we got XML instead of CSV (common for Comext datasets)
            if is_xml_response(csv_data):
                logger.info(f"Received XML response for {dataset_id}, converting to CSV...")
                csv_data = parse_sdmx_xml_to_csv(csv_data)
                if csv_data.startswith("Error") or csv_data.startswith("No data"):
                    results.append(f"- {dataset_id}: FAILED ({csv_data})")
                    failed += 1
                    continue

            with open(output_path, "w", encoding="utf-8", newline="") as f:
                f.write(csv_data)

            row_count = len(csv_data.strip().split("\n")) - 1
            results.append(f"- {dataset_id}: {row_count} rows -> {filename}")
            successful += 1

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP {e.response.status_code}"
            if e.response.status_code == 413:
                error_msg = "Data too large - add more filters"
            results.append(f"- {dataset_id}: FAILED ({error_msg})")
            failed += 1
        except Exception as e:
            results.append(f"- {dataset_id}: FAILED ({str(e)})")
            failed += 1

    summary = f"## Export Summary\n\nDirectory: {output_dir.absolute()}\nSuccessful: {successful}\nFailed: {failed}\n\n### Results:\n" + "\n".join(results)
    return summary

def main():
    logger.info("Starting Eurostat MCP server...")
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()

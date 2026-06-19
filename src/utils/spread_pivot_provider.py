from __future__ import annotations

import json
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def as_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def lay_spread_pivot_mac_dinh(config_cap):
    if "_manual_spread_pivot" in config_cap:
        return as_float(config_cap.get("_manual_spread_pivot"), 0.0)

    spread_pivot = config_cap.get("spread_pivot")
    if spread_pivot is None:
        spread_pivot = config_cap.get("mean", config_cap.get("pivot", 0.0))

    manual_spread_pivot = as_float(spread_pivot, 0.0)
    config_cap["_manual_spread_pivot"] = manual_spread_pivot
    return manual_spread_pivot


def lay_cau_hinh_spread_pivot_tu_dong(config_cap):
    spread_pivot_auto = config_cap.get("spread_pivot_auto")
    if isinstance(spread_pivot_auto, dict):
        return spread_pivot_auto
    return {}


def lay_chu_ky_cap_nhat_spread_pivot(config_cap):
    spread_pivot_auto = lay_cau_hinh_spread_pivot_tu_dong(config_cap)
    if not bool(spread_pivot_auto.get("enabled", False)):
        return 0.0
    return max(1.0, as_float(spread_pivot_auto.get("refresh_second", 60), 60.0))


def dong_bo_spread_pivot_tu_api(config_cap):
    manual_spread_pivot = lay_spread_pivot_mac_dinh(config_cap)
    spread_pivot_auto = lay_cau_hinh_spread_pivot_tu_dong(config_cap)
    if not bool(spread_pivot_auto.get("enabled", False)):
        config_cap["spread_pivot"] = manual_spread_pivot
        config_cap["_spread_pivot_source"] = "config"
        return manual_spread_pivot, "config", "spread_pivot_auto disabled"

    try:
        api_url = str(spread_pivot_auto.get("api_url", "")).strip()
        auth_token = str(spread_pivot_auto.get("auth_token", "")).strip()
        product_id = str(spread_pivot_auto.get("product_id", "")).strip()
        route_base_to_diff = str(
            spread_pivot_auto.get("route_base_to_diff", "")
        ).strip()
        route_diff_to_base = str(
            spread_pivot_auto.get("route_diff_to_base", "")
        ).strip()
        timeout_second = max(
            1.0,
            as_float(spread_pivot_auto.get("timeout_second", 10), 10.0),
        )

        if not api_url:
            raise ValueError("missing spread_pivot_auto.api_url")
        if not route_base_to_diff or not route_diff_to_base:
            raise ValueError(
                "missing spread_pivot_auto.route_base_to_diff or route_diff_to_base"
            )

        query_params = {}
        if product_id:
            query_params["product_id"] = product_id

        request_url = api_url
        if query_params:
            separator = "&" if "?" in request_url else "?"
            request_url = request_url + separator + urlencode(query_params)

        headers = {"Accept": "application/json"}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        request = Request(request_url, headers=headers, method="GET")

        try:
            with urlopen(request, timeout=timeout_second) as response:
                response_body = response.read().decode("utf-8")
        except HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code}: {error_body[:200]}") from exc
        except URLError as exc:
            raise RuntimeError(f"network error: {exc.reason}") from exc

        payload = json.loads(response_body)
        if not payload.get("ok"):
            raise ValueError(
                f"API returned ok=false: {payload.get('error', 'unknown error')}"
            )

        rows = payload.get("rows")
        if not isinstance(rows, list):
            raise ValueError("API payload is missing rows list")

        p98_by_route = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            route = str(row.get("route", "")).strip()
            if not route:
                continue
            p98_by_route[route] = as_float(row.get("p98_spread"), 0.0)

        if route_base_to_diff not in p98_by_route:
            raise ValueError(f"route not found: {route_base_to_diff}")
        if route_diff_to_base not in p98_by_route:
            raise ValueError(f"route not found: {route_diff_to_base}")

        spread_pivot = (
            p98_by_route[route_base_to_diff] + p98_by_route[route_diff_to_base]
        ) / 2.0
        config_cap["spread_pivot"] = spread_pivot
        config_cap["_spread_pivot_source"] = "matrix_api"
        config_cap["_spread_pivot_synced_at"] = time.time()
        return (
            spread_pivot,
            "matrix_api",
            (
                f"{route_base_to_diff}={p98_by_route[route_base_to_diff]:+.5f}, "
                f"{route_diff_to_base}={p98_by_route[route_diff_to_base]:+.5f}"
            ),
        )
    except Exception as exc:
        config_cap["spread_pivot"] = manual_spread_pivot
        config_cap["_spread_pivot_source"] = "config_fallback"
        config_cap["_spread_pivot_last_error"] = str(exc)
        return manual_spread_pivot, "config_fallback", str(exc)

"""
Pinnacle Odds via RapidAPI - simple connector script

Prerequisites:
- Install dependencies: pip install requests
- Get your RapidAPI key from the RapidAPI dashboard after subscribing.

Environment variable:
- RAPIDAPI_KEY should contain your RapidAPI key (or pass --apikey).

Usage examples:
- python api.py --endpoint "/sports"
- python api.py --endpoint "/odds" --params '{"sport":"soccer"}'
- python api.py --endpoint "/events" --apikey YOUR_KEY


Notes:
- Base host: https://pinnacle-odds.p.rapidapi.com
- Required headers:
  - X-RapidAPI-Key: <your key>
  - X-RapidAPI-Host: pinnacle-odds.p.rapidapi.com
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, Optional

try:
	from dotenv import load_dotenv  # type: ignore
	load_dotenv()
except Exception:
	pass

import requests


RAPIDAPI_HOST = "pinnacle-odds.p.rapidapi.com"
BASE_URL = f"https://{RAPIDAPI_HOST}"


def parse_params_json(params_json: Optional[str]) -> Optional[Dict[str, Any]]:
	if not params_json:
		return None
	try:
		parsed: Dict[str, Any] = json.loads(params_json)
		if not isinstance(parsed, dict):
			raise ValueError("--params must be a JSON object, e.g. '{\"sport\":\"soccer\"}'")
		return parsed
	except json.JSONDecodeError as exc:
		raise ValueError(f"Invalid JSON for --params: {exc}") from exc


def ensure_leading_slash(path: str) -> str:
	if not path.startswith("/"):
		return "/" + path
	return path


class PinnacleOddsClient:
	"""
	High-level Python client for Pinnacle Odds (RapidAPI).

	Usage:
		from api import PinnacleOddsClient
		client = PinnacleOddsClient(api_key="YOUR_KEY")
		sports = client.list_sports()
		markets = client.list_markets(sport_id=1, event_type="prematch", is_have_odds=True)
		specials = client.list_specials(sport_id=1)
		archive = client.list_archive_events(sport_id=1)
		details = client.event_details(event_id=123)
		leagues = client.list_leagues(sport_id=1)
		periods = client.meta_periods()
	"""

	def __init__(self, api_key: str, timeout_seconds: float = 20.0) -> None:
		self.api_key = api_key
		self.timeout_seconds = timeout_seconds
		self.headers = {
			"X-RapidAPI-Key": api_key,
			"X-RapidAPI-Host": RAPIDAPI_HOST,
		}

	def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None) -> Any:
		url = BASE_URL + ensure_leading_slash(path)
		if method.upper() == "GET":
			resp = requests.get(url, headers=self.headers, params=params, timeout=self.timeout_seconds)
		elif method.upper() == "POST":
			resp = requests.post(url, headers=self.headers, json=json_body or {}, timeout=self.timeout_seconds)
		else:
			raise ValueError(f"Unsupported method: {method}.")
		resp.raise_for_status()
		try:
			return resp.json()
		except ValueError:
			return resp.text

	# Convenience: convert Python bool to lowercase string if required
	@staticmethod
	def _bool_param(value: Optional[bool]) -> Optional[str]:
		if value is None:
			return None
		return "true" if value else "false"

	# 1) List of sports
	def list_sports(self) -> Any:
		return self._request("GET", "/kit/v1/sports")

	# 2) List of markets by sport_id (supports since, event_type, is_have_odds)
	def list_markets(self, sport_id: int, since: Optional[int] = None, event_type: Optional[str] = None, is_have_odds: Optional[bool] = None) -> Any:
		params: Dict[str, Any] = {"sport_id": sport_id}
		if since is not None:
			params["since"] = since
		if event_type is not None:
			params["event_type"] = event_type
		if is_have_odds is not None:
			params["is_have_odds"] = self._bool_param(is_have_odds)
		return self._request("GET", "/kit/v1/markets", params=params)

	# 3) List of special markets by sport_id (supports since)
	def list_specials(self, sport_id: int, since: Optional[int] = None) -> Any:
		params: Dict[str, Any] = {"sport_id": sport_id}
		if since is not None:
			params["since"] = since
		return self._request("GET", "/kit/v1/specials", params=params)

	# 4) Event list (archive/schedule) by sport_id (supports since)
	def list_archive_events(
		self,
		sport_id: int,
		since: Optional[int] = None,
		league_id: Optional[int] = None,
		page_num: Optional[int] = None,
		page_size: Optional[int] = None,
		season: Optional[str] = None,
		date_from: Optional[str] = None,
		date_to: Optional[str] = None,
	) -> Any:
		params: Dict[str, Any] = {"sport_id": sport_id}
		# Provider requires page_num; default to 1 if not provided
		params["page_num"] = 1 if page_num is None else page_num
		# Optional: page_size if supported by provider
		if page_size is not None:
			params["page_size"] = page_size
		# Some providers may also allow since; include if provided
		if since is not None:
			params["since"] = since
		if league_id is not None:
			params["league_id"] = league_id
		# Optional season or date range filters (provider-specific)
		if season is not None:
			params["season"] = season
		if date_from is not None:
			params["from"] = date_from
		if date_to is not None:
			params["to"] = date_to
		return self._request("GET", "/kit/v1/archive", params=params)

	# 5) Event details (history of odds) by event_id
	def event_details(self, event_id: int) -> Any:
		params: Dict[str, Any] = {"event_id": event_id}
		return self._request("GET", "/kit/v1/details", params=params)

	# Auxiliary endpoints mentioned in docs
	def list_leagues(self, sport_id: Optional[int] = None) -> Any:
		params: Optional[Dict[str, Any]] = None
		if sport_id is not None:
			params = {"sport_id": sport_id}
		return self._request("GET", "/kit/v1/leagues", params=params)

	def meta_periods(self) -> Any:
		return self._request("GET", "/kit/v1/meta-periods")

def call_pinnacle_odds(endpoint: str, api_key: str, method: str = "GET", params: Optional[Dict[str, Any]] = None, timeout_seconds: float = 20.0) -> requests.Response:
	url = BASE_URL + ensure_leading_slash(endpoint)
	headers = {
		"X-RapidAPI-Key": api_key,
		"X-RapidAPI-Host": RAPIDAPI_HOST,
	}
	if method.upper() == "GET":
		response = requests.get(url, headers=headers, params=params, timeout=timeout_seconds)
	elif method.upper() == "POST":
		# If POSTing, send JSON body if provided via params
		response = requests.post(url, headers=headers, json=params or {}, timeout=timeout_seconds)
	else:
		raise ValueError(f"Unsupported method: {method}. Use GET or POST.")

	# Raise on 4xx/5xx to simplify error handling for caller
	response.raise_for_status()
	return response


def main() -> int:
	parser = argparse.ArgumentParser(description="Call Pinnacle Odds endpoints via RapidAPI.")
	parser.add_argument("--endpoint", required=True, help="Endpoint path, e.g. /sports, /odds, /events.")
	parser.add_argument("--method", default="GET", choices=["GET", "POST"], help="HTTP method to use.")
	parser.add_argument("--params", default=None, help="Optional JSON object for query (GET) or body (POST), e.g. '{\"sport\":\"soccer\"}'.")
	parser.add_argument("--apikey", default=(os.getenv("USER_API_KEY") or os.getenv("RAPIDAPI_KEY")), help="RapidAPI key (uses USER_API_KEY or RAPIDAPI_KEY).")
	args = parser.parse_args()

	api_key = args.apikey
	if not api_key:
		print("Error: RapidAPI key not provided. Set USER_API_KEY or RAPIDAPI_KEY in .env or pass --apikey.", file=sys.stderr)
		input("Press Enter to exit...")  # User prefers scripts to wait before closing
		return 1

	try:
		params = parse_params_json(args.params)
		response = call_pinnacle_odds(endpoint=args.endpoint, api_key=api_key, method=args.method, params=params)
		# Try JSON first, then fallback to text
		try:
			data = response.json()
			print(json.dumps(data, indent=2))
		except ValueError:
			print(response.text)
	except requests.HTTPError as http_err:
		print(f"HTTP error: {http_err}", file=sys.stderr)
		if http_err.response is not None:
			print(f"Response content: {http_err.response.text}", file=sys.stderr)
		input("Press Enter to exit...")
		return 2
	except Exception as exc:
		print(f"Error: {exc}", file=sys.stderr)
		input("Press Enter to exit...")
		return 3

	input("Press Enter to exit...")
	return 0


if __name__ == "__main__":
	sys.exit(main())



import asyncio
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from functools import cache
from typing import Dict, List, Optional

import requests
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

BROADCAST_INTERVAL: int = 5
ALERTS_ENDPOINT: str = "https://www.oref.org.il/WarningMessages/alert/alerts.json"
REQUEST_HEADERS: Dict[str, str] = {
    "Referer": "https://www.oref.org.il//12481-he/Pakar.aspx",
    "X-Requested-With": "XMLHttpRequest"
}

logging.basicConfig(
     level=logging.INFO, 
     format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
     datefmt='%H:%M:%S'
 )


@dataclass
class AlertReport:
    id: int
    alerts: Dict[int, List[str]]

    # Get alerts from the oref website.
    @classmethod
    async def get_alerts(cls) -> Optional['AlertReport']:
        logging.debug(f'Fetching alerts...')

        try:
            return cls.from_alert_json(json.loads(
                requests.get(
                    ALERTS_ENDPOINT,
                    headers=REQUEST_HEADERS
                ).content
            ))
        except json.JSONDecodeError as e:
            pass # No alerts...

        # logging.info(f'Empty response.')
        return None

    @classmethod
    def from_alert_json(cls, j):
        alerts = defaultdict(list)
        
        for district in j['data']:
            alerts[_districts()[district]].append(district)

        return AlertReport(
            id=j['id'],
            alerts=dict(alerts)
        )


# Taken from:
# https://github.com/tiangolo/fastapi/discussions/8312#discussioncomment-5150862
class Notifier:
    def __init__(self):
        self.connections: List[WebSocket] = []
        self.generator = self.get_notification_generator()

    async def get_notification_generator(self):
        while True:
            message = yield
            await self._notify(message)

    async def push(self, msg: Optional[str]):
        await self.generator.asend(msg)

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections.append(websocket)

    def remove(self, websocket: WebSocket):
        self.connections.remove(websocket)

    async def _notify(self, message: str):
        living_connections = []
        while len(self.connections) > 0:
            # Looping like this is necessary in case a disconnection is handled
            # during await websocket.send_text(message)
            websocket = self.connections.pop()
            await websocket.send_text(message)
            living_connections.append(websocket)
        self.connections = living_connections


notifier = Notifier()
app = FastAPI()


@cache
def _areas() -> Dict[str, int]:
    # Get page that contains the data.
    areas_json = requests.get('https://www.oref.org.il/12481-he/Pakar.aspx', headers={'Referer': 'https://www.oref.org.il/'})

    # Get all the areas.
    matches = re.findall(r'{ code: \"(\d+)\", area: \"([א-ת ]+)\" }', areas_json.text, flags=re.MULTILINE)

    # Sort the areas.
    matches.sort(key=lambda x: int(x[0]))

    # Create a dictionary with the areas.
    return dict(map(lambda x: (x[1], int(x[0])), matches))

@cache
def _districts() -> Dict[str, int]:
    # Get JSON with the districts.
    districts_json = requests.get('https://www.oref.org.il/Shared/Ajax/GetDistricts.aspx?lang=he').json()

    # Save only the data we need.
    districts = {}
    for district in districts_json:
        districts[district['label']] = int(district['areaid'])

    return districts


@app.get("/areas")
async def areas() -> JSONResponse:
    return JSONResponse(_areas())


@app.get("/districts")
async def districts() -> JSONResponse:
    return JSONResponse(_districts())


@app.websocket("/alerts")
async def websocket_endpoint(websocket: WebSocket):
    await notifier.connect(websocket)

    async def _alive_task(websocket: WebSocket):
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            notifier.remove(websocket)

    await _alive_task(websocket)


@app.on_event('startup')
async def app_startup():
    # Prime the push notification generator
    await notifier.push(None)

    # Run broadcast loop.
    asyncio.create_task(broadcast_loop())


async def broadcast_loop():
    logging.info('Starting alert broadcast loop...')

    last_report_id: int = 0
    # Start a loop to query the event endpoint and send messages to clients.
    while True:
        # Wait before each pool.
        await asyncio.sleep(BROADCAST_INTERVAL)

        # If there are no clients, there is no point to fetch alerts.
        if len(notifier.connections) < 1:
            continue

        # Get the latest alerts.
        report = await AlertReport.get_alerts()
        
        if report: logging.info(f'Report #{report.id}, alerts: {report.alerts}')

        # If the report id is the same as the last one, it's the same report.
        if report is None or report.id == last_report_id:
            continue

        # Update last report id.
        last_report_id = report.id

        # Push alerts notification.
        await notifier.push(
            json.dumps(report.alerts)
        )

        logging.info(f'{len(report.alerts)} alerts sent to {len(notifier.connections)} client!')

"""Pretend to be a person (IT Support, technician, ...) doing a step in the DCR graph.

One step:
    python do_step.py 06183 RegisterFault

A whole repair, step by step (press Enter between steps):
    python do_step.py 06183 --repair

The result (accepted or refused) shows up in the microservice logs in Docker Desktop.
"""

import argparse
import json
from urllib.request import Request, urlopen

SIDDHI_URL = "http://localhost:7071/equipment"

# What each event ID in the graph means
EVENTS = {
    "RegisterFault": "Register fault ticket",
    "OutOfService": "Take station out of service",
    "Repair": "Repair instrument",
    "Verify": "Verify data",
    "Reactivate": "Reactivate station",
    "CloseTicket": "Close fault ticket",
}

# A full repair where the station is taken offline while it is fixed
REPAIR_STEPS = [
    "RegisterFault",
    "OutOfService",
    "Repair",
    "Verify",
    "Reactivate",
    "CloseTicket",
]


def send_step(station_id, event_id):
    body = {"stationId": station_id, "action": event_id}
    request = Request(
        SIDDHI_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        print(f"Sent: {station_id} -> {event_id} ({EVENTS.get(event_id, '?')})  [HTTP {response.status}]")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("station", help="station ID, e.g. 06183")
    parser.add_argument("event", nargs="?", help="event ID, e.g. RegisterFault")
    parser.add_argument("--repair", action="store_true", help="walk through a whole repair")
    args = parser.parse_args()

    if args.repair:
        for event_id in REPAIR_STEPS:
            input(f"Press Enter to do: {EVENTS[event_id]} ")
            send_step(args.station, event_id)
    elif args.event in EVENTS:
        send_step(args.station, args.event)
    else:
        print("Unknown event. Choose one of these:")
        for event_id, name in EVENTS.items():
            print(f"  {event_id:15} {name}")


if __name__ == "__main__":
    main()

from flask import Flask, request, jsonify
import json
import requests
from datetime import datetime, timezone
from dcr_helper import create_case, execute_event, get_enabled_events

app = Flask(__name__)

SIDDHI_CALLBACK_URL = "http://siddhi:7072/middleman"

# The IDs of the events in your DCR graph on dcrgraphs.net.
# They must be spelled exactly like the IDs in the graph.
ALLOWED_EVENTS = {
    # Alarms (sent by Siddhi)
    "DetectMissing", "FlagSuspect",
    # Human steps (sent by do_step.py)
    "RegisterFault", "OutOfService", "Repair", "Verify", "Reactivate", "CloseTicket",
}


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def read_body():
    """Siddhi wraps the data like {"event": {...}}, so unwrap it if needed."""
    data = request.get_json(silent=True) or {}
    return data.get("event", data)


def send_callback_to_siddhi(station_id, case_id, event_name, enabled_events, error=""):
    """Tell Siddhi what happened, and which events the DCR graph allows next."""
    payload = {
        "stationId": station_id or "",
        "case_id": case_id or "",
        "event": event_name or "",
        "timestamp": utc_now_iso(),
        "role": "microservice",
        "payload": json.dumps({
            "enabledEvents": enabled_events,
            "status": "error" if error else "success",
        }),
        "error": error,
    }
    print(f"[Callback] {payload}")
    requests.post(SIDDHI_CALLBACK_URL, json=payload)


@app.route("/execute", methods=["POST"])
def execute():
    data = read_body()
    station_id = data.get("stationId")
    action = data.get("action")
    case_id = data.get("case_id")
    print(f"[Execute] station={station_id} action={action} case_id={case_id}")

    if action not in ALLOWED_EVENTS:
        send_callback_to_siddhi(station_id, case_id, action, [], f"Unknown event '{action}'")
        return jsonify({"status": "unknown event"}), 400

    # No case yet for this station: start a new instance of the DCR graph
    if not case_id:
        case_id = create_case()
        if case_id is None:
            send_callback_to_siddhi(station_id, "", action, [], "Could not create a DCR case")
            return jsonify({"status": "could not create case"}), 500

    # Ask the DCR graph to execute the event. If the graph's rules don't allow it,
    # dcrgraphs.net refuses and result["error"] is True.
    result = execute_event(case_id, action)
    enabled = get_enabled_events(case_id)

    error = ""
    if result.get("error"):
        error = f"DCR refused '{action}' ({result.get('message')})"

    send_callback_to_siddhi(station_id, case_id, action, enabled, error)
    return jsonify({"status": "ok", "case_id": case_id, "enabledEvents": enabled})


@app.route("/enabled_events/<case_id>", methods=["GET"])
def enabled_events(case_id):
    """Open http://localhost:5001/enabled_events/<case_id> in a browser to see what is allowed now."""
    return jsonify({"case_id": case_id, "enabled_events": get_enabled_events(case_id)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)

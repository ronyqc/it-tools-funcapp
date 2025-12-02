import logging
import json
import uuid
import os

import azure.functions as func
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError

# Function App (modelo v2 con decoradores)
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# --- Configuración de Table Storage ---
CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
TABLE_NAME = "Tickets"

# Creamos el cliente de tabla
table_service = TableServiceClient.from_connection_string(CONNECTION_STRING)
table_client = table_service.get_table_client(TABLE_NAME)


def json_response(body: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body),
        status_code=status_code,
        mimetype="application/json"
    )


# 1) create_ticket - ahora persiste en Table Storage
@app.route(route="create_ticket", methods=["POST"])
def create_ticket(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("create_ticket called")

    try:
        body = req.get_json()
    except ValueError:
        return json_response({"error": "Invalid JSON body"}, 400)

    user_id = body.get("user_id")
    issue_description = body.get("issue_description")

    if not user_id or not issue_description:
        return json_response(
            {"error": "Missing required fields: user_id, issue_description"},
            400
        )

    ticket_id = f"INC-{uuid.uuid4().hex[:8].upper()}"
    status = "OPEN"

    entity = {
        "PartitionKey": "Tickets",
        "RowKey": ticket_id,
        "user_id": user_id,
        "issue_description": issue_description,
        "status": status
    }

    try:
        table_client.upsert_entity(entity)
        logging.info(f"[TABLE] Ticket saved: {ticket_id}")
    except Exception as e:
        logging.error(f"[TABLE] Error saving ticket {ticket_id}: {e}")
        # IMPORTANTE: no decirle al agente que se creó si falló el insert
        return json_response(
            {"error": "Error saving ticket in storage"},
            500
        )

    return json_response(
        {"ticket_id": ticket_id, "status": status},
        200
    )


# 2) get_ticket_status - lee desde Table Storage
@app.route(route="get_ticket_status", methods=["POST"])
def get_ticket_status(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("get_ticket_status called")

    try:
        body = req.get_json()
    except ValueError:
        return json_response({"error": "Invalid JSON body"}, 400)

    ticket_id = body.get("ticket_id")
    if not ticket_id:
        return json_response({"error": "Missing required field: ticket_id"}, 400)

    try:
        entity = table_client.get_entity(
            partition_key="Tickets",
            row_key=ticket_id
        )
        status = entity.get("status", "UNKNOWN")
        logging.info(f"[TABLE] Ticket found: {ticket_id} -> {status}")
        return json_response(
            {"ticket_id": ticket_id, "status": status},
            200
        )

    except ResourceNotFoundError:
        logging.warning(f"[TABLE] Ticket not found: {ticket_id}")
        return json_response({"error": "Ticket not found"}, 404)


# 3) send_notification - igual que antes (mock)
@app.route(route="send_notification", methods=["POST"])
def send_notification(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("send_notification called")

    try:
        body = req.get_json()
    except ValueError:
        return json_response({"error": "Invalid JSON body"}, 400)

    user_id = body.get("user_id")
    message = body.get("message")

    if not user_id or not message:
        return json_response(
            {"error": "Missing required fields: user_id, message"},
            400
        )

    logging.info(f"[NOTIFICATION] To user {user_id}: {message}")

    return json_response({"success": True}, 200)


# 4) start_provisioning_workflow - sigue mock
@app.route(route="start_provisioning_workflow", methods=["POST"])
def start_provisioning_workflow(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("start_provisioning_workflow called")

    try:
        body = req.get_json()
    except ValueError:
        return json_response({"error": "Invalid JSON body"}, 400)

    user_id = body.get("user_id")
    request_type = body.get("request_type")

    if not user_id or not request_type:
        return json_response(
            {"error": "Missing required fields: user_id, request_type"},
            400
        )

    workflow_id = f"WF-{uuid.uuid4().hex[:8].upper()}"
    logging.info(
        f"[WORKFLOW] Starting workflow {workflow_id} "
        f"for user {user_id} with request_type={request_type}"
    )

    return json_response(
        {"workflow_id": workflow_id, "started": True},
        200
    )

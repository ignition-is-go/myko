from enum import Enum
from typing import Any, Dict

import uuid
from datetime import datetime, timezone

class MEventType(Enum):
    SET = 'SET'
    DEL = 'DEL'

class MItem:
    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name
        self.hash = None

class MEvent:
    def __init__(self, item: MItem, item_type: str, change_type: MEventType, created_at: str, tx: str):
        self.item = item.__dict__
        self.itemType = item_type
        self.changeType = change_type.value
        self.createdAt = created_at
        self.tx = tx

class WSMEvent:
    def __init__(self, data: MEvent):
        self.event = "ws:m:event"
        self.data = data.__dict__

class MQuery:
    def __init__(self, id: str):
        self.id = id

class MWrappedQuery:
    def __init__(self, query_id: str, query_item_type: str, query: MQuery, tx: str):
        self.queryId = query_id
        self.queryItemType = query_item_type
        self.query = query.__dict__
        self.tx = tx

class MCommand:
    def __init__(self, tx: str, created_at: str):
        self.tx = tx
        self.createdAt = created_at

class WSMCommand:
    def __init__(self, command_id: str, client_id: str, data: MCommand):
        self.event = 'ws:m:command'
        self.data = {
            'commandId': command_id,
            'clientId': client_id,
            'command': data.__dict__
        }

class MWrappedCommand:
    def __init__(self, command_id: str, client_id: str, data: MCommand):
        self.command_id = command_id
        self.client_id = client_id
        self.data = data.__dict__


def generate_id() -> str:
    return str(uuid.uuid4())


def get_current_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


class Schema:
    def __init__(self, schema_type: str, properties: Dict[str, Any]):
        self.type = schema_type
        self.properties = properties


class SchemaProperty:
    def __init__(self, property_type: str):
        self.type = property_type



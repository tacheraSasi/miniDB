from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from exceptions import SchemaValidationError

class Field:
    def __init__(self, field_type: type, required: bool = True, default: Any = None):
        self.field_type = field_type
        self.required = required
        self.default = default

    def validate(self, value: Any) -> Any:
        if value is None:
            if self.required:
                raise SchemaValidationError(f"Required field cannot be None")
            return self.default

        if not isinstance(value, self.field_type):
            try:
                return self.field_type(value)
            except (ValueError, TypeError):
                raise SchemaValidationError(
                    f"Expected {self.field_type.__name__}, got {type(value).__name__}"
                )
        return value

class Schema:
    def __init__(self, **fields: Dict[str, Field]):
        self.fields = fields

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        validated = {}
        
        # Check for required fields
        for field_name, field in self.fields.items():
            if field_name not in data:
                if field.required:
                    raise SchemaValidationError(f"Missing required field: {field_name}")
                if field.default is not None:
                    validated[field_name] = field.default
                continue
            
            validated[field_name] = field.validate(data[field_name])

        # Check for unknown fields
        unknown_fields = set(data.keys()) - set(self.fields.keys())
        if unknown_fields:
            raise SchemaValidationError(f"Unknown fields: {unknown_fields}")

        return validated

# Field type definitions
class IntegerField(Field):
    def __init__(self, required: bool = True, default: Optional[int] = None,
                 min_value: Optional[int] = None, max_value: Optional[int] = None):
        super().__init__(int, required, default)
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, value: Any) -> int:
        value = super().validate(value)
        if value is not None:
            if self.min_value is not None and value < self.min_value:
                raise SchemaValidationError(f"Value {value} is less than minimum {self.min_value}")
            if self.max_value is not None and value > self.max_value:
                raise SchemaValidationError(f"Value {value} is greater than maximum {self.max_value}")
        return value

class StringField(Field):
    def __init__(self, required: bool = True, default: Optional[str] = None,
                 min_length: Optional[int] = None, max_length: Optional[int] = None):
        super().__init__(str, required, default)
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, value: Any) -> str:
        value = super().validate(value)
        if value is not None:
            if self.min_length is not None and len(value) < self.min_length:
                raise SchemaValidationError(f"String length {len(value)} is less than minimum {self.min_length}")
            if self.max_length is not None and len(value) > self.max_length:
                raise SchemaValidationError(f"String length {len(value)} is greater than maximum {self.max_length}")
        return value

class DateTimeField(Field):
    def __init__(self, required: bool = True, default: Optional[datetime] = None,
                 auto_now: bool = False, auto_now_add: bool = False):
        super().__init__(datetime, required, default)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add

    def validate(self, value: Any) -> datetime:
        if self.auto_now or (self.auto_now_add and value is None):
            return datetime.now()
        return super().validate(value)
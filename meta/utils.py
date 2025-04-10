from typing import Annotated, TypeVar

from pydantic import BeforeValidator, HttpUrl, TypeAdapter, BaseModel

http_url_adapter = TypeAdapter(HttpUrl)
Url = Annotated[str, BeforeValidator(lambda value: str(http_url_adapter.validate_python(value)))]


T = TypeVar('T', bound=BaseModel)


def merge_models(base: T, override: T) -> T:
    assert type(base) == type(override)
    merged_data = {}
    for field, field_info in base.__class__.model_fields.items():
        base_val = getattr(base, field)
        override_val = getattr(override, field)
        if isinstance(base_val, dict) and isinstance(override_val, dict):
            merged = {}
            for k in list(base_val.keys()) + list(override_val.keys()):
                if k in merged:
                    # already handled before
                    continue
                if k not in base_val:
                    merged[k] = override_val[k]
                    continue
                base_v = base_val[k]
                if k not in override_val:
                    merged[k] = base_v
                    continue
                override_v = override_val[k]
                if isinstance(base_v, BaseModel):
                    merged[k] = merge_models(base_v, override_v)
                else:
                    # merged[k] = {**base_v, **override_v} # untested
                    raise NotImplementedError()
            merged_data[field] = merged
        elif isinstance(base_val, BaseModel) and isinstance(override_val, BaseModel):
            # Recursively merge nested models
            merged_data[field] = merge_models(base_val, override_val)
        elif override_val is not None:
            merged_data[field] = override_val
        else:
            merged_data[field] = base_val
    return base.__class__(**merged_data)

def optional_entry[K: object, V: object](key: K, value: V) -> dict[K, V]:
    if value is None:
        return {}
    else:
        return {key: value}

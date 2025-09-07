import json
import hashlib

"""Hashing personal data."""


def delete_personal_data(entity_response, fields):
    """Delete personal data from entity_response dictionary."""
    for field in fields:
        field_value = entity_response.get(field) 
        if field_value is not None:
            if type(field_value) is str:
                entity_response[field] = sha256_hash(field_value)
            else: 
                entity_response[field] = ''

    return entity_response 


def sha256_hash(text: str) -> str:
    """Hashes a string using SHA-256 and returns the hex representation."""
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


# coding: utf-8

def merge_dict(d1, d2):
    return {
        key: merge_dict(d1[key], d2[key]) if key in d2 and key in d1 and isinstance(d1[key], dict) and isinstance(d2[key], dict) else
             d2[key] if key in d2 else
             d1[key]
        for key in set(d1.keys()) | set(d2.keys())
    }

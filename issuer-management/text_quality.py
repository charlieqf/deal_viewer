"""Detect broken PDF character maps independently of text length and image area."""
def broken_character_map(text):
    chars = ''.join(str(text).split())
    if len(chars) < 100:
        return False
    unusual = sum('\u0100' <= c <= '\u1fff' or '\u3400' <= c <= '\u4dbf' for c in chars)
    return unusual / len(chars) > .12

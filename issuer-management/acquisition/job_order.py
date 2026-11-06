"""Share a bounded daily batch between document types; rating backlogs cannot starve risk."""
from collections import defaultdict, deque


def fair_order(jobs):
    groups=defaultdict(deque)
    for item in jobs:groups[item['kind']].append(item)
    result=[]
    while any(groups.values()):
        for kind in sorted(groups):
            if groups[kind]:result.append(groups[kind].popleft())
    return result

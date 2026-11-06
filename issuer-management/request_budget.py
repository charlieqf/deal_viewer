"""Application request ceilings; Gateway account limits remain authoritative."""


def daily_limit(config):
    # Production backlog runs continuously; Gateway entitlements stay authoritative.
    return None if config.get('pipeline') == 'issuer-production-v1' else 400


def check_budget(root_attempts, daily_attempts, config):
    if root_attempts >= 36:
        raise RuntimeError('Issuer request limit reached: 36 attempts per root')
    limit = daily_limit(config)
    if limit is not None and daily_attempts >= limit:
        raise RuntimeError(f'Application daily request limit reached: {limit}; wait for next UTC day')

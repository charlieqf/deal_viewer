"""Application request ceilings; Gateway account limits remain authoritative."""


def daily_limit(config):
    # The implementation plan budgets 450/day for production; keep the pilot at 400.
    return 450 if config.get('pipeline') == 'issuer-production-v1' else 400


def check_budget(root_attempts, daily_attempts, config):
    if root_attempts >= 36:
        raise RuntimeError('Issuer request limit reached: 36 attempts per root')
    limit = daily_limit(config)
    if daily_attempts >= limit:
        raise RuntimeError(f'Application daily request limit reached: {limit}; wait for next UTC day')

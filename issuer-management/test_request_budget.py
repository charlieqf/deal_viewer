import unittest
from request_budget import check_budget, daily_limit


class RequestBudgetTests(unittest.TestCase):
    def test_pilot_and_unknown_configs_keep_lower_limit(self):
        for config in ({}, {'pipeline':'issuer-pilot-v2'}, {'pipeline':'other'}):
            self.assertEqual(daily_limit(config), 400)
            check_budget(35,399,config)
            with self.assertRaises(RuntimeError):check_budget(35,400,config)

    def test_production_continues_past_old_daily_cap_with_finite_root_retries(self):
        config={'pipeline':'issuer-production-v1'}
        self.assertIsNone(daily_limit(config))
        for daily in (449,450,451,10000):check_budget(35,daily,config)
        with self.assertRaises(RuntimeError):check_budget(36,0,config)


if __name__=='__main__':unittest.main()
